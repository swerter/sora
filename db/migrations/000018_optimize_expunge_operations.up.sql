-- Migration 018: Optimize maintain_message_sequences trigger for EXPUNGE operations
-- Eliminates costly O(N) sequence table drop-and-rebuild during expunge by performing incremental adjustments.

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
        -- For UPDATE, only run if sequencing-related columns changed.
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

        -- Check if there are any cross-mailbox moves
        EXECUTE '
            SELECT EXISTS (
                SELECT 1
                FROM old_table o
                JOIN new_table n ON o.id = n.id
                WHERE o.mailbox_id IS DISTINCT FROM n.mailbox_id
            )
        ' INTO should_run;

        IF NOT should_run THEN
            -- Only pure expunge/restore operations (no mailbox_id changes)
            -- We can do an incremental shift instead of a full rebuild
            FOR v_mailbox_id IN SELECT DISTINCT mailbox_id FROM old_table WHERE mailbox_id IS NOT NULL LOOP
                PERFORM pg_advisory_xact_lock(v_mailbox_id);

                -- Handle message RESTORE operations explicitly by doing a full rebuild
                -- Restores are extremely rare (admin tools only) so rebuilding immediately is preferable.
                EXECUTE '
                    SELECT EXISTS (
                        SELECT 1 FROM old_table o JOIN new_table n ON o.id = n.id
                        WHERE o.mailbox_id = $1 AND o.expunged_at IS NOT NULL AND n.expunged_at IS NULL
                    )
                ' INTO should_run USING v_mailbox_id;

                IF should_run THEN
                    -- Full Rebuild
                    DELETE FROM message_sequences WHERE mailbox_id = v_mailbox_id;
                    INSERT INTO message_sequences (mailbox_id, uid, seqnum)
                    SELECT m.mailbox_id, m.uid, ROW_NUMBER() OVER (ORDER BY m.uid)
                    FROM messages m
                    WHERE m.mailbox_id = v_mailbox_id AND m.expunged_at IS NULL;
                    
                    CONTINUE;
                END IF;

                -- 1. DELETE the explicitly expunged records from message_sequences
                -- We use old_table and new_table statement state safely.
                DELETE FROM message_sequences ms
                USING old_table o, new_table n
                WHERE ms.mailbox_id = v_mailbox_id
                  AND o.mailbox_id = v_mailbox_id
                  AND ms.uid = o.uid
                  AND o.id = n.id
                  AND o.expunged_at IS NULL AND n.expunged_at IS NOT NULL;

                -- 2. Calculate offsets and temporarily mutate shifting rows to NEGATIVE
                -- This structurally avoids ANY unique constraint collisions between the old seqnums 
                -- and the dynamically shifting seqnums while the UPDATE is physically processing.
                WITH shift_offsets AS (
                    SELECT ms.uid,
                           ms.seqnum - ROW_NUMBER() OVER (ORDER BY ms.seqnum) AS shift_amount
                    FROM message_sequences ms
                    WHERE ms.mailbox_id = v_mailbox_id
                )
                UPDATE message_sequences ms
                SET seqnum = -(ms.seqnum - so.shift_amount)
                FROM shift_offsets so
                WHERE ms.mailbox_id = v_mailbox_id
                  AND ms.uid = so.uid
                  AND so.shift_amount > 0;

                -- 3. Flip them back to their absolute positive gaps in a clean pass natively
                UPDATE message_sequences
                SET seqnum = -seqnum
                WHERE mailbox_id = v_mailbox_id AND seqnum < 0;

            END LOOP;
            
            RETURN NULL;
        END IF;

        -- Mailbox changes (moves): fall back to full rebuild
        affected_mailboxes_query := '
            SELECT DISTINCT mailbox_id FROM new_table WHERE mailbox_id IS NOT NULL
            UNION
            SELECT DISTINCT mailbox_id FROM old_table WHERE mailbox_id IS NOT NULL';
    END IF;

    -- Full rebuild for all affected mailboxes (Fallback for bulk insert/delete/moves)
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
