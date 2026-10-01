-- Recreate the seven indexes dropped by 000051, with their original definitions
-- (000001 for the sort indexes and account_mailbox_hash, 000006 for to_display_sort), and
-- drop the replacement idx_messages_mailbox_active.
--
-- On a large database build them out-of-band first with CREATE INDEX CONCURRENTLY; every
-- statement below is IF NOT EXISTS and then no-ops. A plain CREATE INDEX here holds a
-- SHARE lock on messages (blocks writes) for the whole build.

CREATE INDEX IF NOT EXISTS idx_messages_subject_sort ON messages (mailbox_id, subject_sort) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_from_email_sort ON messages (mailbox_id, from_email_sort) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_from_display_sort ON messages (mailbox_id, COALESCE(from_name_sort, from_email_sort)) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_to_email_sort ON messages (mailbox_id, to_email_sort) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_to_display_sort ON messages (mailbox_id, COALESCE(to_name_sort, to_email_sort)) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_cc_email_sort ON messages (mailbox_id, cc_email_sort) WHERE expunged_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_messages_account_mailbox_hash ON messages (account_id, mailbox_id, content_hash);

DROP INDEX IF EXISTS idx_messages_mailbox_active;
