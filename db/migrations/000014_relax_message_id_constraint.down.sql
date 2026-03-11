DROP INDEX IF EXISTS idx_messages_message_id_mailbox_lookup;

-- Restore the partial unique index
CREATE UNIQUE INDEX IF NOT EXISTS messages_message_id_mailbox_id_active_idx 
    ON messages(message_id, mailbox_id) WHERE expunged_at IS NULL;
