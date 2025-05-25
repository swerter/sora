package db

import (
	"context"
	"log"

	"github.com/emersion/go-imap/v2"
)

func (db *Database) ExpungeMessageUIDs(ctx context.Context, mailboxID int64, uids ...imap.UID) error {
	if len(uids) == 0 {
		log.Printf("[EXPUNGE] no UIDs to expunge for mailbox %d", mailboxID)
		return nil
	}

	log.Printf("[EXPUNGE] expunging %d messages from mailbox %d: %v", len(uids), mailboxID, uids)

	result, err := db.Pool.Exec(ctx, `
		UPDATE messages
		SET expunged_at = NOW(), expunged_modseq = nextval('messages_modseq')
		WHERE mailbox_id = $1 AND uid = ANY($2) AND expunged_at IS NULL
	`, mailboxID, uids)

	if err != nil {
		log.Printf("[EXPUNGE] error expunging messages: %v", err)
		return err
	}

	rowsAffected := result.RowsAffected()
	log.Printf("[EXPUNGE] successfully expunged %d messages from mailbox %d", rowsAffected, mailboxID)

	// Double-check that the messages were actually expunged
	var count int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) 
		FROM messages 
		WHERE mailbox_id = $1 AND uid = ANY($2) AND expunged_at IS NULL
	`, mailboxID, uids).Scan(&count)

	if err != nil {
		log.Printf("[EXPUNGE] error checking if messages were expunged: %v", err)
	} else if count > 0 {
		log.Printf("[EXPUNGE] WARNING: %d messages were not expunged", count)
	}

	return nil
}
