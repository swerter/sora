-- Rollback migration 000011: Re-create account storage index

-- This rollback re-creates the index that was removed.
-- Note: The index is not needed for query performance (storage is cached in mailbox_stats),
-- but this rollback allows reverting to the previous state if needed.

CREATE INDEX IF NOT EXISTS idx_messages_account_storage
    ON messages (account_id, size)
    WHERE expunged_at IS NULL;
