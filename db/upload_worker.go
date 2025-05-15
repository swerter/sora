package db

import (
	"context"
	"fmt"
	"time"

	"github.com/migadu/sora/consts"
)

type PendingUpload struct {
	ID          int64
	ContentHash string // serves also as unique identifier in S3
	InstanceID  string // hostname of the instance that created the upload
	Size        int64
	Attempts    int
}

func (db *Database) ListPendingUploads(ctx context.Context, instanceId string, limit int) ([]PendingUpload, error) {
	lastAttemptAfter := time.Now().Add(-consts.PENDING_UPLOAD_RETRY_INTERVAL)
	rows, err := db.Pool.Query(ctx, `
		SELECT id, content_hash, size, instance_id, attempts
		FROM pending_uploads
		WHERE instance_id = $1
		  AND (attempts < $3)
		  AND ((last_attempt IS NULL) OR (last_attempt < $4))
		ORDER BY created_at
		LIMIT $2
	`, instanceId, limit, consts.MAX_UPLOAD_ATTEMPTS, lastAttemptAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []PendingUpload
	for rows.Next() {
		var u PendingUpload
		if err := rows.Scan(&u.ID, &u.ContentHash, &u.Size, &u.InstanceID, &u.Attempts); err != nil { // Scan order matches SELECT
			return nil, err
		}
		uploads = append(uploads, u)
	}
	return uploads, nil
}

// MarkUploadAttempt increments the attempt count for a pending upload.
// This is called when an upload attempt fails.
func (db *Database) MarkUploadAttempt(ctx context.Context, contentHash string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE pending_uploads
		SET attempts = attempts + 1, last_attempt = now()
		WHERE content_hash = $1`, contentHash)
	return err
}

// CompleteS3Upload marks all messages with the given content hash as uploaded
// and deletes the specific pending upload record.
// This is called by an upload worker after successfully uploading the content hash to S3.
func (db *Database) CompleteS3Upload(ctx context.Context, contentHash string) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE messages 
		SET uploaded = TRUE
		WHERE content_hash = $1 AND uploaded = FALSE
	`, contentHash)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM pending_uploads 
		WHERE content_hash = $1
	`, contentHash)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// IsContentHashUploaded checks if any message with the given content hash is already marked as uploaded.
// This is used by workers to avoid redundant S3 uploads.
func (db *Database) IsContentHashUploaded(ctx context.Context, contentHash string) (bool, error) {
	var uploaded bool
	err := db.Pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM messages WHERE content_hash = $1 AND uploaded = TRUE)
	`, contentHash).Scan(&uploaded)
	if err != nil {
		return false, fmt.Errorf("failed to check if content hash %s is uploaded: %w", contentHash, err)
	}
	return uploaded, nil
}
