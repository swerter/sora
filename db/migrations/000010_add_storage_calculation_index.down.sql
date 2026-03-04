-- Rollback migration 000010: Remove storage calculation index

DROP INDEX IF EXISTS idx_messages_account_storage;
