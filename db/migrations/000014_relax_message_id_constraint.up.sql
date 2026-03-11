-- Drop the partial unique index that enforces one Message-ID per active mailbox
-- This allows multiple drafts or messages with the same Message-ID to exist
-- concurrently, which is standard IMAP behavior.

-- NOTE: This migration uses regular CREATE INDEX and DROP INDEX because golang-migrate
-- wraps migrations in a transaction, which prevents using the CONCURRENTLY keyword.
-- This can block writes/reads on the messages table for the duration of index creation.
-- 
-- For zero-downtime deployments on large databases, run these steps manually FIRST:
--   1. CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_message_id_mailbox_lookup 
--        ON messages(mailbox_id, message_id) WHERE expunged_at IS NULL;
--   2. DROP INDEX CONCURRENTLY IF EXISTS messages_message_id_mailbox_id_active_idx;
--
-- Then, when this migration runs, the DROP and CREATE will be instant/no-ops.

-- Add standard non-unique index for fast lookups by Message-ID within a mailbox
CREATE INDEX IF NOT EXISTS idx_messages_message_id_mailbox_lookup
  ON messages(mailbox_id, message_id) WHERE expunged_at IS NULL;

-- Drop the old unique index
DROP INDEX IF EXISTS messages_message_id_mailbox_id_active_idx;

-- Drop the old unique constraint (just in case it still exists from v1 schema)
ALTER TABLE messages DROP CONSTRAINT IF EXISTS messages_message_id_mailbox_id_key;
