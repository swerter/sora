-- Replace seven indexes on messages that no query plan needs with one small one (verified
-- 2026-09-30 against production scan statistics and EXPLAIN of every consumer with the
-- index removed; the write-up is in the local tasks/messages-index-drop-verification.md).
--
-- The six (mailbox_id, <sort column>) partial indexes were meant for IMAP SORT. The planner
-- never uses them for that: a mailbox's active rows are read and sorted in memory,
-- whichever indexes exist. The only work they did in production was to serve as the
-- smallest "active rows of mailbox X" path for whole-mailbox scans (the To/Cc ones have
-- billions of scans for that reason alone: their values repeat within a mailbox, so btree
-- deduplication keeps them tiny). The index built for that lookup,
-- idx_messages_mailbox_id_expunged_at_is_null, was dropped in 000031 as a prefix of
-- (mailbox_id, uid) WHERE expunged_at IS NULL; that one cannot deduplicate (uid is unique)
-- and is ~8x larger per mailbox, so falling back to it would cost more index pages per scan
-- than today. idx_messages_mailbox_active below restores a deduplicated single-column
-- path (~1/8 the size of the uid index) BEFORE the sort indexes go.
--
-- idx_messages_account_mailbox_hash (account_id, mailbox_id, content_hash) has one exact
-- consumer, the importer's force-reimport delete, which now runs on
-- idx_messages_content_hash_account_id with a filter on mailbox_id: a (hash, account) pair
-- has a handful of rows. Its account_id prefix was also serving the few account-only
-- statements (accountHasMessages, accountFinalizableSQL, the purge-domain object listers).
-- Those move to idx_messages_s3_key_parts, which is now the ONLY non-partial index leading
-- with account_id and must stay for as long as those statements carry no expunged_at
-- predicate. Do not drop it in a later migration without either adding a plain (account_id)
-- index first or rewriting those four statements onto the account_hash_active /
-- account_hash_expunged partial indexes.
--
-- Deliberately NOT dropped here:
--   idx_messages_expunged_null_created_at  sole consumer is ExpungeOldMessages, which only
--                                          runs with max_age_restriction enabled; without
--                                          it that query is a table scan per batch.
--   idx_messages_message_id                SEARCH HEADER Message-ID would become a
--                                          whole-mailbox heap scan.
--   idx_messages_in_reply_to, idx_messages_recipients_json  same fallback for two rare
--                                          searches; kept for now.
--
-- On a large production database do this out-of-band first, off-peak, on the primary:
--   1. CREATE INDEX CONCURRENTLY idx_messages_mailbox_active ON messages (mailbox_id)
--        WHERE expunged_at IS NULL;
--   2. watch pg_stat_user_indexes for a while: scans on the To/Cc sort indexes should stop
--      and appear on the new index;
--   3. DROP INDEX CONCURRENTLY each of the seven, one at a time.
-- Every statement below is IF [NOT] EXISTS and then no-ops. Run inside this migration on a
-- large table, the CREATE INDEX holds a SHARE lock on messages (blocks writes) for the
-- build, and each DROP INDEX takes an ACCESS EXCLUSIVE lock that queues behind any long
-- transaction on the table.

CREATE INDEX IF NOT EXISTS idx_messages_mailbox_active ON messages (mailbox_id) WHERE expunged_at IS NULL;

DROP INDEX IF EXISTS idx_messages_subject_sort;
DROP INDEX IF EXISTS idx_messages_from_email_sort;
DROP INDEX IF EXISTS idx_messages_from_display_sort;
DROP INDEX IF EXISTS idx_messages_to_email_sort;
DROP INDEX IF EXISTS idx_messages_to_display_sort;
DROP INDEX IF EXISTS idx_messages_cc_email_sort;
DROP INDEX IF EXISTS idx_messages_account_mailbox_hash;
