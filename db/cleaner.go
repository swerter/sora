package db

import (
	"context"
	"fmt"
)

const CLEANUP_LOCK_KEY = 987654321 // Used by all instances

func (d *Database) AcquireCleanupLock(ctx context.Context) (bool, error) {
	var success bool
	err := d.Pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, CLEANUP_LOCK_KEY).Scan(&success)
	return success, err
}

func (d *Database) ReleaseCleanupLock(ctx context.Context) {
	_, _ = d.Pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, CLEANUP_LOCK_KEY)
}

func (d *Database) DeleteExpungedMessagesByContentHash(ctx context.Context, contentHash string) error {
	_, err := d.Pool.Exec(ctx, `
		DELETE 
			FROM messages
		WHERE 
			content_hash = $1 AND 
			expunged_at IS NOT NULL
	`, contentHash)
	if err != nil {
		return fmt.Errorf("failed to delete expunged messages for hash %s: %w", contentHash, err)
	}
	return nil
}
