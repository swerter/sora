CREATE OR REPLACE FUNCTION update_message_contents_tsvector() RETURNS trigger AS $$
DECLARE
    sanitized_body text;
    safe_body text;
    safe_hdrs text;
BEGIN
    -- Handle text_body
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        IF NEW.text_body IS NOT NULL THEN
            IF NEW.text_body IS DISTINCT FROM OLD.text_body THEN
                -- Replace backslashes with spaces to prevent PostgreSQL "unsupported Unicode
                -- escape sequence" errors (SQLSTATE 22P05) in to_tsvector().
                safe_body := replace(NEW.text_body, E'\\', ' ');
                NEW.text_body_tsv := strip(to_tsvector('simple', safe_body));
                NEW.text_body := NULL;
            ELSIF OLD.text_body IS NOT NULL THEN
                NEW.text_body_tsv := OLD.text_body_tsv;
            END IF;
        END IF;

        IF NEW.headers IS DISTINCT FROM OLD.headers THEN
            IF NEW.headers IS NOT NULL AND NEW.headers != '' THEN
                safe_hdrs := replace(NEW.headers, E'\\', ' ');
                NEW.headers_tsv := strip(to_tsvector('simple', safe_hdrs));
            ELSIF OLD.headers != '' THEN
                NEW.headers_tsv := OLD.headers_tsv;
            END IF;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
