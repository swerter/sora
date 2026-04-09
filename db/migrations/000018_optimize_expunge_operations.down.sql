-- Migration 018: Revert sequence optimizations for EXPUNGE

CREATE OR REPLACE FUNCTION maintain_message_sequences()
RETURNS TRIGGER AS
$$
DECLARE
    v_mailbox_id BIGINT;
    affected_mailboxes_query TEXT;
    should_run BOOLEAN;
    v_insert_count INT;
    v_has_expunged BOOLEAN;
    v_new_uid BIGINT;
    v_max_existing_uid BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- OPTIMIZATION: For single-message inserts of non-expunged messages,
        -- do an incremental update instead of a full rebuild.
        -- This covers the most common case: LMTP delivery and APPEND.
        SELECT COUNT(*), bool_or(expunged_at IS NOT NULL)
        INTO v_insert_count, v_has_expunged
        FROM new_table;

        IF v_insert_count = 1 AND NOT COALESCE(v_has_expunged, FALSE) THEN
            -- Get the new message's mailbox and UID
            SELECT mailbox_id, uid INTO v_mailbox_id, v_new_uid
            FROM new_table WHERE mailbox_id IS NOT NULL;

            IF v_mailbox_id IS NOT NULL THEN
                PERFORM pg_advisory_xact_lock(v_mailbox_id);

                -- Check if new UID is higher than all existing UIDs (common case: APPEND/LMTP)
                -- For imports with --preserve-uids or restoration, UIDs may not be monotonic,
                -- so we must fall through to a full rebuild in those cases.
                SELECT MAX(uid) INTO v_max_existing_uid
                FROM message_sequences WHERE mailbox_id = v_mailbox_id;

                IF v_new_uid > COALESCE(v_max_existing_uid, 0) THEN
                    -- Fast path: new message has the highest UID, just append with next seqnum
                    INSERT INTO message_sequences (mailbox_id, uid, seqnum)
                    VALUES (v_mailbox_id, v_new_uid,
                            COALESCE((SELECT MAX(seqnum) FROM message_sequences WHERE mailbox_id = v_mailbox_id), 0) + 1)
                    ON CONFLICT (mailbox_id, uid) DO NOTHING;

                    RETURN NULL;
                END IF;
                -- Non-monotonic UID: fall through to full rebuild
            END IF;
        END IF;

        -- Multi-message insert or has expunged: fall through to full rebuild
        affected_mailboxes_query := 'SELECT DISTINCT mailbox_id FROM new_table WHERE mailbox_id IS NOT NULL';

    ELSIF TG_OP = 'DELETE' THEN
        affected_mailboxes_query := 'SELECT DISTINCT mailbox_id FROM old_table WHERE mailbox_id IS NOT NULL';

    ELSE -- TG_OP = 'UPDATE'
        -- For UPDATE, only run the rebuild if sequencing-related columns changed.
        EXECUTE '
            SELECT EXISTS (
                SELECT 1
                FROM old_table o
                JOIN new_table n ON o.id = n.id
                WHERE o.mailbox_id IS DISTINCT FROM n.mailbox_id
                   OR (o.expunged_at IS NULL) IS DISTINCT FROM (n.expunged_at IS NULL)
            )
        ' INTO should_run;

        IF NOT should_run THEN
            RETURN NULL;
        END IF;

        affected_mailboxes_query := '
            SELECT DISTINCT mailbox_id FROM new_table WHERE mailbox_id IS NOT NULL
            UNION
            SELECT DISTINCT mailbox_id FROM old_table WHERE mailbox_id IS NOT NULL';
    END IF;

    -- Full rebuild for all affected mailboxes
    FOR v_mailbox_id IN EXECUTE affected_mailboxes_query LOOP
        PERFORM pg_advisory_xact_lock(v_mailbox_id);

        DELETE FROM message_sequences WHERE mailbox_id = v_mailbox_id;
        INSERT INTO message_sequences (mailbox_id, uid, seqnum)
        SELECT m.mailbox_id, m.uid, ROW_NUMBER() OVER (ORDER BY m.uid)
        FROM messages m
        WHERE m.mailbox_id = v_mailbox_id AND m.expunged_at IS NULL;
    END LOOP;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
