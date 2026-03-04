-- Migration 000010: Add index for storage calculation queries
-- This index optimizes the storage_stats CTE in ListAccounts and ListAccountsByDomain
-- which calculates total storage usage per account for non-expunged messages.

-- Partial index for account storage calculations (account_id, size) for non-expunged messages
-- Used in: ListAccounts, ListAccountsByDomain
-- Query pattern: SELECT account_id, SUM(size) FROM messages WHERE expunged_at IS NULL GROUP BY account_id

-- NOTE: This uses regular CREATE INDEX (not CONCURRENTLY) because golang-migrate
-- wraps migrations in a transaction. This takes a SHARE lock on messages table,
-- blocking writes for the duration of index creation.
-- For zero-downtime deployments on large databases, create the index manually first:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_account_storage
--       ON messages (account_id, size) WHERE expunged_at IS NULL;
-- Then this migration becomes a no-op due to IF NOT EXISTS.
CREATE INDEX IF NOT EXISTS idx_messages_account_storage
    ON messages (account_id, size)
    WHERE expunged_at IS NULL;
