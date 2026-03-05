-- Rollback migration 012: drop partial pruning indexes
DROP INDEX IF EXISTS idx_message_contents_bodies_prunable;
DROP INDEX IF EXISTS idx_message_contents_vectors_prunable;
