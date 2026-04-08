-- Optimize FTS vector generation by moving to trigger
-- This prevents blocking INSERT operations with inline to_tsvector() calls

-- Create trigger function to generate FTS vectors.
-- Note: the application layer enforces a 64KB storage cap on text_body and headers
-- (values larger than 64KB are stored as NULL/'' before reaching the database).
-- Therefore no truncation is needed here — values are either NULL/empty (handled
-- by the source-pruning branches) or already within a safe size for to_tsvector().
--
-- Lifecycle:
--   INSERT        → compute TSVs from source text (then null text_body immediately —
--                   it is never persisted; the trigger clears it after computing the vector).
--                   headers are kept as long as the row exists (controlled by fts_retention).
--   UPDATE (body set to non-null) → recompute text_body_tsv, then null text_body immediately.
--   UPDATE (headers set to non-null) → recompute headers_tsv. headers are NOT nulled by the
--                   trigger — they remain in the row for IMAP fast-path header fetches until
--                   fts_retention expires (PruneOldMessageVectors deletes the entire row).
--   UPDATE (body/headers set to NULL/'')  → PRESERVE existing TSVs so the message
--     remains FTS-searchable until fts_retention expires (PruneOldMessageVectors).
--   PruneOldMessageVectors UPDATE (text_body_tsv/headers_tsv set to NULL)
--     → trigger does NOT fire (column filter only covers text_body, headers),
--       so explicit NULLs in that UPDATE are written as-is.
CREATE OR REPLACE FUNCTION update_message_contents_tsvector() RETURNS TRIGGER AS $$
DECLARE
    safe_body  text;
    safe_hdrs  text;
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Fresh insert: compute TSVs from source text.
        -- NULL body → NULL TSV (not empty tsvector) so the vectors_prunable partial
        -- index (WHERE tsv IS NOT NULL) correctly excludes un-indexed rows.
        -- Compute TSVs. Use simple configuration for general text search without stemming,
        -- as we want prefix-search to work predictably.
        -- `strip()` removes positional information, saving ~50% storage space for the tsvector
        IF NEW.text_body IS NOT NULL THEN
            -- Replace backslashes with spaces to prevent PostgreSQL "unsupported Unicode
            -- escape sequence" errors (SQLSTATE 22P05) in to_tsvector().
            safe_body := replace(NEW.text_body, E'\\', ' ');
            NEW.text_body_tsv := strip(to_tsvector('simple', safe_body));
            -- text_body is never persisted: clear it immediately after computing the vector.
            NEW.text_body := NULL;
        ELSE
            NEW.text_body_tsv := NULL;
        END IF;

        IF NEW.headers IS NOT NULL AND NEW.headers != '' THEN
            safe_hdrs := replace(NEW.headers, E'\\', ' ');
            NEW.headers_tsv := strip(to_tsvector('simple', safe_hdrs));
        ELSE
            NEW.headers_tsv := NULL;
        END IF;

    ELSE -- UPDATE OF text_body, headers
        -- Recompute text_body_tsv only when source is being written (non-null).
        -- When source is pruned (NULL), preserve the existing TSV so the message
        -- stays FTS-searchable until PruneOldMessageVectors removes it later.
        IF NEW.text_body IS DISTINCT FROM OLD.text_body THEN
            IF NEW.text_body IS NOT NULL THEN
                safe_body := replace(NEW.text_body, E'\\', ' ');
                NEW.text_body_tsv := strip(to_tsvector('simple', safe_body));
                -- text_body is never persisted: clear it immediately after computing the vector.
                NEW.text_body := NULL;
            ELSIF OLD.text_body IS NOT NULL THEN
                -- Source is being pruned: carry forward the existing search vector.
                NEW.text_body_tsv := OLD.text_body_tsv;
            -- else: both already NULL — leave text_body_tsv unchanged.
            END IF;
        END IF;

        -- Same logic for headers_tsv.
        IF NEW.headers IS DISTINCT FROM OLD.headers THEN
            IF NEW.headers IS NOT NULL AND NEW.headers != '' THEN
                safe_hdrs := replace(NEW.headers, E'\\', ' ');
                NEW.headers_tsv := strip(to_tsvector('simple', safe_hdrs));
            ELSIF OLD.headers != '' THEN
                -- Headers being pruned: carry forward the existing search vector.
                NEW.headers_tsv := OLD.headers_tsv;
            -- else: both already empty — leave headers_tsv unchanged.
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and re-create trigger to pick up the updated function
DROP TRIGGER IF EXISTS message_contents_tsvector_update ON message_contents;

-- Create trigger that fires before INSERT or UPDATE
CREATE TRIGGER message_contents_tsvector_update
    BEFORE INSERT OR UPDATE OF text_body, headers ON message_contents
    FOR EACH ROW
    EXECUTE FUNCTION update_message_contents_tsvector();

-- Note: text_body is now NEVER persisted — the trigger clears it immediately after
-- computing text_body_tsv. This means:
--   - No fts_source_retention cleanup is needed (no body to prune).
--   - headers remain in the row until fts_retention expires (PruneOldMessageVectors deletes
--     the entire row), which also handles IMAP fast-path header fetches.
--   - FTS search continues to work via text_body_tsv and headers_tsv.
