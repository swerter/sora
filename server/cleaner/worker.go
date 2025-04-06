package cleaner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/migadu/sora/db"
	"github.com/migadu/sora/storage"
)

type CleanupWorker struct {
	db          *db.Database
	s3          *storage.S3Storage
	interval    time.Duration
	gracePeriod time.Duration
}

func New(db *db.Database, s3 *storage.S3Storage, interval, gracePeriod time.Duration) *CleanupWorker {
	return &CleanupWorker{
		db:          db,
		s3:          s3,
		interval:    interval,
		gracePeriod: gracePeriod,
	}
}

func (w *CleanupWorker) Start(ctx context.Context) {
	log.Printf("Cleanup worker starting with interval: %v", w.interval)
	interval := w.interval
	if interval < time.Second {
		log.Printf("WARNING: Cleanup interval too small (%v), using 1h fallback", interval)
		interval = time.Hour
	}
	ticker := time.NewTicker(w.interval)
	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("S3 cleanup worker stopped")
				return
			case <-ticker.C:
				log.Println("Running S3 cleanup")
				if err := w.runOnce(ctx); err != nil {
					log.Printf("Cleanup error: %v", err)
				}
			}
		}
	}()
}

func (w *CleanupWorker) runOnce(ctx context.Context) error {
	locked, err := w.db.AcquireCleanupLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	if !locked {
		log.Println("Cleanup skipped: another instance is running it")
		return nil
	}
	defer w.db.ReleaseCleanupLock(ctx)

	const batchSize = 100

	candidates, err := w.db.ListS3ObjectsToDelete(ctx, w.gracePeriod, batchSize)
	if err != nil {
		return fmt.Errorf("failed to list S3 delete candidates: %w", err)
	}

	if len(candidates) == 0 {
		log.Println("No more S3 objects to clean up")
		return nil
	}

	for _, c := range candidates {
		s3Key := fmt.Sprintf("%s/%s/%s", c.Domain, c.LocalPart, c.UUID)
		log.Printf("Deleting S3 object: %s", s3Key)

		if err := w.s3.DeleteMessage(s3Key); err != nil {
			log.Printf("Failed to delete S3 object %s: %v", s3Key, err)
			continue
		}

		if err := w.db.DeleteExpungedMessagesByUUID(ctx, c.UUID); err != nil {
			log.Printf("Failed to delete DB rows for UUID %s: %v", c.UUID, err)
			continue
		}

		log.Printf("Successfully deleted UUID %s", c.UUID)
	}

	return nil
}
