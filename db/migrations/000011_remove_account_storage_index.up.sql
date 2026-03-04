-- Migration 000011: Remove deprecated account storage index
--
-- The idx_messages_account_storage index created in migration 000010 is no longer needed.
-- Storage calculations have been optimized to use mailbox_stats.total_size (cached data
-- maintained by triggers) instead of scanning the messages table.
--
-- This significantly improves performance:
--   BEFORE: Scan all messages in messages table, filter by expunged_at, aggregate by account_id
--   AFTER:  Join small tables (mailboxes + mailbox_stats), sum pre-calculated total_size
--
-- The index was causing unnecessary maintenance overhead (updates on every message operation)
-- without providing any performance benefit after the query optimization.

-- For production deployments, drop the index without blocking:
-- Run this manually BEFORE applying the migration:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_messages_account_storage;

DROP INDEX IF EXISTS idx_messages_account_storage;
