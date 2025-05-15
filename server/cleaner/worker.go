package cleaner

// Package cleaner provides a worker that periodically cleans up S3 objects
// that are no longer needed, based on a grace period defined in the database.
// It uses a database advisory lock to ensure that only one instance of the
// cleanup worker is running at a time. The cleanup process involves listing
// S3 objects that are candidates for deletion and removing them from both
// S3 and the database. The worker runs at a specified interval, which can
// be configured to a minimum of 1 hour if set too small. The grace period
// is the time after which S3 objects are considered for deletion. The
// worker is designed to be started in a separate goroutine and will
// continue running until the context is done. It logs its progress and
// any errors encountered during the cleanup process.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/migadu/sora/cache"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/storage"
	"github.com/minio/minio-go/v7"
)

const BATCH_PURGE_SIZE = 100

type CleanupWorker struct {
	db          *db.Database
	s3          *storage.S3Storage
	cache       *cache.Cache
	interval    time.Duration
	gracePeriod time.Duration
}

func New(db *db.Database, s3 *storage.S3Storage, cache *cache.Cache, interval, gracePeriod time.Duration) *CleanupWorker {
	return &CleanupWorker{
		db:          db,
		s3:          s3,
		cache:       cache,
		interval:    interval,
		gracePeriod: gracePeriod,
	}
}

func (w *CleanupWorker) Start(ctx context.Context) {
	log.Printf("[CLEANUP] worker starting with interval: %v", w.interval)
	interval := w.interval
	const minInterval = time.Minute
	if interval < minInterval {
		log.Printf("[CLEANUP] WARNING: interval %v is less than minimum %v, using minimum.", interval, minInterval)
		interval = minInterval
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("[CLEANUP] worker stopped")
				return
			case <-ticker.C:
				log.Println("[CLEANUP] running S3 cleanup")
				if err := w.runOnce(ctx); err != nil {
					log.Printf("[CLEANUP] error: %v", err)
				}
			}
		}
	}()
}

func (w *CleanupWorker) runOnce(ctx context.Context) error {
	locked, err := w.db.AcquireCleanupLock(ctx)
	if err != nil {
		log.Println("[CLEANUP] failed to acquire advisory lock:", err)
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	if !locked {
		log.Println("[CLEANUP] skipped: another instance is running it")
		return nil
	}
	defer w.db.ReleaseCleanupLock(ctx)

	candidates, err := w.db.ListS3ObjectsToDelete(ctx, w.gracePeriod, BATCH_PURGE_SIZE)
	if err != nil {
		log.Println("[CLEANUP] failed to list delete candidates:", err)
		return fmt.Errorf("failed to list S3 delete candidates: %w", err)
	}

	if len(candidates) == 0 {
		log.Println("[CLEANUP] no objects to clean up")
		return nil
	}

	for _, cHash := range candidates {
		log.Printf("[CLEANUP] deleting object: %s", cHash)

		s3Err := w.s3.Delete(cHash)

		// Check if the error indicates the object was not found (HTTP 404)
		isS3ObjectNotFoundError := false
		var minioErr minio.ErrorResponse
		if s3Err != nil && errors.As(s3Err, &minioErr) {
			if minioErr.StatusCode == 404 {
				isS3ObjectNotFoundError = true
			}
		}

		// If S3 deletion failed AND it was NOT a 'not found' error, log and skip DB delete.
		if s3Err != nil && !isS3ObjectNotFoundError {
			log.Printf("[CLEANUP] failed to delete object %s: %v", cHash, s3Err)
			continue // Skip to the next candidate
		}

		if err := w.db.DeleteExpungedMessagesByContentHash(ctx, cHash); err != nil {
			// Log the error but continue processing other candidates
			// This is important to ensure that we don't stop the entire cleanup
			// process if one deletion fails. We don't retry here as the
			// advisory lock is already held and we don't want to block other
			// cleanup processes. Next run will pick it up.
			log.Printf("[CLEANUP] failed to delete DB rows for hash %s: %v", cHash, err)
			continue
		}

		if err := w.cache.Delete(cHash); err != nil {
			log.Printf("[CLEANUP] failed to delete from cache for hash %s: %v", cHash, err)
			// Continue processing other candidates, cache will expire
			// eventually if not deleted
			continue
		}

		log.Printf("[CLEANUP] deleted hash %s", cHash)
	}

	return nil
}
