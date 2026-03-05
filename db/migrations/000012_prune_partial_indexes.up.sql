-- Migration 012: Partial indexes to speed up incremental FTS body/vector pruning
--
-- PROBLEM:
--   The batched pruning queries scan message_contents in primary-key order and
--   check (text_body IS NOT NULL OR headers != '') for every row they encounter,
--   including rows that have already been pruned (text_body = NULL, headers = '').
--   On a large table where most rows are already pruned, each 100-row batch
--   requires touching thousands of heap pages to find the next prunable rows,
--   resulting in ~9 seconds per batch and only ~3 300 rows pruned per 15-minute
--   cleanup window.
--
-- FIX:
--   Two partial indexes covering only the un-pruned rows allow PostgreSQL to
--   directly access candidates without scanning the entire table.  As rows are
--   pruned they fall out of the partial index automatically, so subsequent runs
--   are always fast regardless of table size.
--
-- ZERO-DOWNTIME DEPLOYMENT:
--   On a large production table, create these indexes CONCURRENTLY first
--   (outside a transaction) to avoid a long write-lock:
--
--     psql -d DATABASE -c "
--       CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_message_contents_bodies_prunable
--         ON message_contents (content_hash)
--         WHERE text_body IS NOT NULL OR headers != '';
--
--       CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_message_contents_vectors_prunable
--         ON message_contents (content_hash)
--         WHERE text_body_tsv IS NOT NULL OR headers_tsv IS NOT NULL;
--     "
--
--   Then run golang-migrate normally.  The IF NOT EXISTS clauses below are
--   no-ops when the indexes already exist.

-- ============================================================================
-- 1. PARTIAL INDEX FOR BODY PRUNING
-- ============================================================================
-- Used by PruneOldMessageBodiesBatched:
--
--   SELECT mc.content_hash
--   FROM message_contents mc
--   WHERE (mc.text_body IS NOT NULL OR mc.headers != '')
--     AND NOT EXISTS (SELECT 1 FROM messages m WHERE ...)
--   ORDER BY mc.content_hash
--   LIMIT 100
--
-- Without this index PostgreSQL scans the full table in PK order checking
-- text_body on every heap page.  With it, the scan is limited to only the
-- un-pruned rows (which shrinks with each cleanup cycle).
CREATE INDEX IF NOT EXISTS idx_message_contents_bodies_prunable
    ON message_contents (content_hash)
    WHERE text_body IS NOT NULL OR headers != '';

-- ============================================================================
-- 2. PARTIAL INDEX FOR VECTOR PRUNING
-- ============================================================================
-- Used by PruneOldMessageVectorsBatched:
--
--   SELECT mc.content_hash
--   FROM message_contents mc
--   WHERE (mc.text_body_tsv IS NOT NULL OR mc.headers_tsv IS NOT NULL)
--     AND NOT EXISTS (SELECT 1 FROM messages m WHERE ...)
--   ORDER BY mc.content_hash
--   LIMIT 100
CREATE INDEX IF NOT EXISTS idx_message_contents_vectors_prunable
    ON message_contents (content_hash)
    WHERE text_body_tsv IS NOT NULL OR headers_tsv IS NOT NULL;
