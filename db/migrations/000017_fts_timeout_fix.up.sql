CREATE OR REPLACE FUNCTION update_message_contents_tsvector() RETURNS trigger AS $$
DECLARE
    sanitized_body text;
    safe_body text;
    safe_hdrs text;
BEGIN
    -- Handle text_body
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        IF NEW.text_body IS NOT NULL THEN
            -- Only process if it changed (or is new)
            IF NEW.text_body IS DISTINCT FROM OLD.text_body THEN
                -- Clean up PostgreSQL null bytes (0x00) which break tsvector parsing
                sanitized_body := replace(NEW.text_body, '\', '\\');
                sanitized_body := replace(sanitized_body, CHR(0), ' ');

                -- Cap total parsing size to ~100KB to prevent OOM
                safe_body := substring(sanitized_body for 100000);

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
