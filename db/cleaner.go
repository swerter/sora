package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
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

func (d *Database) DeleteExpungedMessagesByUUID(ctx context.Context, uuid uuid.UUID) error {
	_, err := d.Pool.Exec(ctx, `
		DELETE FROM messages
		WHERE uuid = $1 AND expunged_at IS NOT NULL
	`, uuid)
	if err != nil {
		return fmt.Errorf("failed to delete expunged messages for UUID %s: %w", uuid.String(), err)
	}
	return nil
}
