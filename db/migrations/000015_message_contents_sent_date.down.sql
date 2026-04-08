DROP INDEX IF EXISTS idx_message_contents_sent_date;
ALTER TABLE message_contents DROP COLUMN IF EXISTS sent_date;
