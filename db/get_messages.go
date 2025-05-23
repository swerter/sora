package db

import (
	"context"
	"fmt"
	"log"

	"github.com/emersion/go-imap/v2"
	"github.com/jackc/pgx/v5"
	"github.com/migadu/sora/helpers"
)

func (db *Database) GetMessagesByNumSet(ctx context.Context, mailboxID int64, numSet imap.NumSet) ([]Message, error) {
	log.Printf("[GetMessagesByNumSet]: fetching messages for mailbox %d with set %v", mailboxID, numSet)

	if uidSet, ok := numSet.(imap.UIDSet); ok {
		messages, err := db.getMessagesByUIDSet(ctx, mailboxID, uidSet)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	if seqSet, ok := numSet.(imap.SeqSet); ok {
		messages, err := db.getMessagesBySeqSet(ctx, mailboxID, seqSet)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}

	return nil, fmt.Errorf("unsupported NumSet type: %T", numSet)
}

func (db *Database) getMessagesByUIDSet(ctx context.Context, mailboxID int64, uidSet imap.UIDSet) ([]Message, error) {
	var messages []Message

	for _, uidRange := range uidSet {
		// Base query is the same for all cases
		baseQuery := `
			WITH numbered_messages AS (
				SELECT 
					user_id, uid, mailbox_id, content_hash, uploaded, flags, 
					internal_date, size, body_structure,
					created_modseq, updated_modseq, expunged_modseq,
					row_number() OVER (ORDER BY id) AS seqnum
				FROM messages
				WHERE mailbox_id = $1 AND expunged_at IS NULL
			)
			SELECT 
				user_id, uid, mailbox_id, content_hash, uploaded, flags, 
				internal_date, size, body_structure,
				created_modseq, updated_modseq, expunged_modseq, seqnum
			FROM numbered_messages
			WHERE uid >= $2
		`

		var rows pgx.Rows
		var err error
		var rangeDesc string

		// Add upper bound condition for non-wildcard queries
		if uidRange.Stop == imap.UID(0) {
			// Wildcard range (e.g., 1:*)
			rangeDesc = fmt.Sprintf("%d:*", uidRange.Start)
			query := baseQuery + " ORDER BY seqnum"
			rows, err = db.Pool.Query(ctx, query, mailboxID, uint32(uidRange.Start))
		} else {
			// Regular range (e.g., 1:5)
			rangeDesc = fmt.Sprintf("%d:%d", uidRange.Start, uidRange.Stop)
			query := baseQuery + " AND uid <= $3 ORDER BY seqnum"
			rows, err = db.Pool.Query(ctx, query, mailboxID, uint32(uidRange.Start), uint32(uidRange.Stop))
		}

		if err != nil {
			return nil, fmt.Errorf("failed to query messages with UID range %s: %v", rangeDesc, err)
		}

		rangeMessages, err := scanMessages(rows)
		if err != nil {
			return nil, err
		}

		messages = append(messages, rangeMessages...)
	}

	log.Printf("Fetched %d messages with UID ranges", len(messages))
	return messages, nil
}

func (db *Database) getMessagesBySeqSet(ctx context.Context, mailboxID int64, seqSet imap.SeqSet) ([]Message, error) {
	if len(seqSet) == 1 && seqSet[0].Start == 1 && (seqSet[0].Stop == 0) {
		log.Printf("SeqSet includes all messages (1:*) for mailbox %d", mailboxID)
		return db.fetchAllActiveMessagesRaw(ctx, mailboxID)
	}

	var allMessages []Message

	for _, seqRange := range seqSet {
		stopSeq := seqRange.Stop

		query := `
			WITH numbered_messages AS (
				SELECT 
					user_id, uid, mailbox_id, content_hash, uploaded, flags, 
					internal_date, size, body_structure,
					created_modseq, updated_modseq, expunged_modseq,
					row_number() OVER (ORDER BY id) AS seqnum
				FROM messages
				WHERE mailbox_id = $1 AND expunged_at IS NULL
			)
			SELECT 
				user_id, uid, mailbox_id, content_hash, uploaded, flags, 
				internal_date, size, body_structure,
				created_modseq, updated_modseq, expunged_modseq, seqnum
			FROM numbered_messages
			WHERE seqnum >= $2 AND seqnum <= $3
			ORDER BY seqnum
		`

		rows, err := db.Pool.Query(ctx, query, mailboxID, seqRange.Start, stopSeq)
		if err != nil {
			return nil, fmt.Errorf("failed to query messages for sequence range %d:%d: %v",
				seqRange.Start, stopSeq, err)
		}

		rangeMessages, err := scanMessages(rows)
		if err != nil {
			return nil, err
		}

		allMessages = append(allMessages, rangeMessages...)
	}

	log.Printf("Fetched %d messages for SeqSet %v", len(allMessages), seqSet)
	return allMessages, nil
}

func (db *Database) fetchAllActiveMessagesRaw(ctx context.Context, mailboxID int64) ([]Message, error) {
	query := `
		WITH numbered_messages AS (
			SELECT 
				user_id, uid, mailbox_id, content_hash, uploaded, flags, 
				internal_date, size, body_structure,
				created_modseq, updated_modseq, expunged_modseq,
				row_number() OVER (ORDER BY id) AS seqnum
			FROM messages
			WHERE mailbox_id = $1 AND expunged_at IS NULL
		)
		SELECT 
			user_id, uid, mailbox_id, content_hash, uploaded, flags, 
			internal_date, size, body_structure,
			created_modseq, updated_modseq, expunged_modseq, seqnum
		FROM numbered_messages
		ORDER BY seqnum
	`
	rows, err := db.Pool.Query(ctx, query, mailboxID)
	if err != nil {
		return nil, fmt.Errorf("fetchAllActiveMessagesRaw: failed to query: %w", err)
	}
	return scanMessages(rows)
}

func scanMessages(rows pgx.Rows) ([]Message, error) {
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var bodyStructureBytes []byte
		if err := rows.Scan(&msg.UserID, &msg.UID, &msg.MailboxID, &msg.ContentHash, &msg.IsUploaded,
			&msg.BitwiseFlags, &msg.InternalDate, &msg.Size, &bodyStructureBytes, &msg.CreatedModSeq,
			&msg.UpdatedModSeq, &msg.ExpungedModSeq, &msg.Seq); err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		bodyStructure, err := helpers.DeserializeBodyStructureGob(bodyStructureBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize BodyStructure: %v", err)
		}
		msg.BodyStructure = *bodyStructure
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error scanning rows: %v", err)
	}

	return messages, nil
}
