-- Rollback FTS trigger optimization

DROP TRIGGER IF EXISTS message_contents_tsvector_update ON message_contents;
DROP FUNCTION IF EXISTS update_message_contents_tsvector();

-- Note: After rolling back this migration, application code must be reverted
-- to include explicit to_tsvector() calls in INSERT/UPDATE statements.
