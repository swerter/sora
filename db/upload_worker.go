package db

import (
	"context"

	"github.com/google/uuid"
)

type PendingUpload struct {
	ID          int64
	MessageID   int64
	UUID        uuid.UUID
	FilePath    string
	S3Path      string
	Size        int64
	MailboxName string
	DomainName  string
	LocalPart   string
}

func (db *Database) ListPendingUploads(ctx context.Context, limit int) ([]PendingUpload, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, message_id, uuid, file_path, s3_path, size, mailbox_name, domain_name, local_part
		FROM pending_uploads
		ORDER BY created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []PendingUpload
	for rows.Next() {
		var u PendingUpload
		var uuidStr string
		if err := rows.Scan(&u.ID, &u.MessageID, &uuidStr, &u.FilePath, &u.S3Path, &u.Size, &u.MailboxName, &u.DomainName, &u.LocalPart); err != nil {
			return nil, err
		}
		u.UUID, err = uuid.Parse(uuidStr)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, u)
	}
	return uploads, nil
}

func (db *Database) MarkUploadAttempt(ctx context.Context, id int64, success bool) error {
	if success {
		return nil // handled via DeletePendingUpload
	}
	_, err := db.Pool.Exec(ctx, `
		UPDATE pending_uploads
		SET attempts = attempts + 1, last_attempt = now()
		WHERE id = $1
	`, id)
	return err
}

func (db *Database) CompleteS3Upload(ctx context.Context, messageID, uploadID int64) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE messages SET s3_uploaded = TRUE WHERE id = $1
	`, messageID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM pending_uploads WHERE id = $1
	`, uploadID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
