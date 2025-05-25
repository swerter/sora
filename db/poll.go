package db

import (
	"context"
	"encoding/json"
	"fmt"

	"database/sql"

	"github.com/emersion/go-imap/v2"
	"github.com/jackc/pgx/v5"
)

type MessageUpdate struct {
	UID          imap.UID
	SeqNum       uint32
	BitwiseFlags int // Matches the 'flags' column type in messages table (INTEGER)
	IsExpunge    bool
	CustomFlags  []string
}

type MailboxPoll struct {
	Updates     []MessageUpdate
	NumMessages uint32
	ModSeq      uint64
}

func (db *Database) PollMailbox(ctx context.Context, mailboxID int64, sinceModSeq uint64) (*MailboxPoll, error) {
	// Use a transaction to ensure we have a consistent view of the mailbox
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT * FROM (
			WITH
			  global_stats AS (
			    SELECT
			      (SELECT COUNT(m_count.uid) FROM messages m_count WHERE m_count.mailbox_id = $1 AND m_count.expunged_at IS NULL) AS total_messages,
			      (SELECT lv.last_value FROM messages_modseq lv) AS current_modseq
			  ),
			  current_mailbox_state AS (
			    SELECT
			        m.uid,
			        ROW_NUMBER() OVER (ORDER BY m.id) AS seq_num,
			        m.flags,
			        m.custom_flags,
			        m.created_modseq,
			        m.updated_modseq,
			        m.expunged_modseq
			    FROM messages m
			    WHERE m.mailbox_id = $1 AND (m.expunged_modseq IS NULL OR m.expunged_modseq > $2) -- $2 is sinceModSeq
			  ),
			  changed_messages AS (
			    SELECT
			        cms.uid,
			        cms.seq_num,
			        cms.flags,
			        cms.custom_flags,
			        cms.expunged_modseq,
			        true AS is_message_update
			    FROM current_mailbox_state cms
			    WHERE
			        (cms.created_modseq <= $2 AND cms.updated_modseq > $2 AND cms.expunged_modseq IS NULL) OR
			        (cms.created_modseq <= $2 AND cms.expunged_modseq > $2)
			  )
			SELECT
			    cm.uid,
			    cm.seq_num,
			    cm.flags,
			    cm.custom_flags,
			    cm.expunged_modseq,
			    cm.is_message_update,
			    gs.total_messages,
			    gs.current_modseq
			FROM changed_messages cm, global_stats gs
			UNION ALL
			SELECT
			    NULL AS uid,
			    NULL AS seq_num,
			    NULL AS flags,
			    NULL AS custom_flags,
			    NULL AS expunged_modseq,
			    false AS is_message_update,
			    gs.total_messages,
			    gs.current_modseq
			FROM global_stats gs
			WHERE NOT EXISTS (SELECT 1 FROM changed_messages)
		) AS combined_results
		ORDER BY
		    is_message_update DESC,             -- Process actual message updates first
		    (expunged_modseq IS NOT NULL) DESC, -- Process expunges before flag changes within message updates
		    CASE WHEN expunged_modseq IS NOT NULL THEN -seq_num ELSE seq_num END ASC -- Expunges by seq_num DESC, others by seq_num ASC
	`, mailboxID, sinceModSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to query combined mailbox poll: %w", err)
	}
	defer rows.Close()

	var updates []MessageUpdate
	var pollData MailboxPoll
	firstRowProcessed := false

	for rows.Next() {
		var (
			uidScannable          sql.NullInt32 // imap.UID is uint32
			seqNumScannable       sql.NullInt32 // uint32
			bitwiseFlagsScannable sql.NullInt32 // INTEGER in DB
			expungedModSeqPtr     *int64
			customFlagsJSON       []byte
			isMessageUpdate       bool
			rowTotalMessages      uint32
			rowCurrentModSeq      uint64
		)

		err := rows.Scan(
			&uidScannable,
			&seqNumScannable,
			&bitwiseFlagsScannable,
			&customFlagsJSON,
			&expungedModSeqPtr,
			&isMessageUpdate,
			&rowTotalMessages,
			&rowCurrentModSeq,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan combined poll data: %w", err)
		}

		if !firstRowProcessed {
			pollData.NumMessages = rowTotalMessages
			pollData.ModSeq = rowCurrentModSeq
			firstRowProcessed = true
		}

		if isMessageUpdate {
			if !uidScannable.Valid || !seqNumScannable.Valid || !bitwiseFlagsScannable.Valid {
				return nil, fmt.Errorf("unexpected NULL value in message update row: uid_valid=%v, seq_valid=%v, flags_valid=%v", uidScannable.Valid, seqNumScannable.Valid, bitwiseFlagsScannable.Valid)
			}
			var customFlags []string
			if customFlagsJSON != nil { // Will be []byte("[]") if empty, or []byte("null")
				if err := json.Unmarshal(customFlagsJSON, &customFlags); err != nil {
					return nil, fmt.Errorf("failed to unmarshal custom_flags in poll: %w, json: %s", err, string(customFlagsJSON))
				}
			}
			updates = append(updates, MessageUpdate{
				UID:          imap.UID(uidScannable.Int32),
				SeqNum:       uint32(seqNumScannable.Int32),
				BitwiseFlags: int(bitwiseFlagsScannable.Int32),
				IsExpunge:    expungedModSeqPtr != nil,
				CustomFlags:  customFlags,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through combined poll results: %w", err)
	}

	pollData.Updates = updates
	return &pollData, nil
}
