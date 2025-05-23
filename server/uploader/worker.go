package uploader

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/migadu/sora/cache"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/storage"
)

type UploadWorker struct {
	db         *db.Database
	s3         *storage.S3Storage
	cache      *cache.Cache
	tempPath   string
	instanceID string // New field
	notifyCh   chan struct{}
	errCh      chan<- error
}

func New(ctx context.Context, tempPath string, instanceID string, db *db.Database, s3 *storage.S3Storage, cache *cache.Cache, errCh chan<- error) (*UploadWorker, error) {
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		if err := os.MkdirAll(tempPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create local path %s: %w", tempPath, err)
		}
	}
	notifyCh := make(chan struct{}, 1)

	return &UploadWorker{
		db:         db,
		s3:         s3,
		cache:      cache,
		errCh:      errCh,
		tempPath:   tempPath,
		instanceID: instanceID,
		notifyCh:   notifyCh,
	}, nil
}

func (w *UploadWorker) Start(ctx context.Context) {
	log.Println("Starting upload worker")

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("[UPLOADER] worker stopped")
				return
			case <-ticker.C:
				log.Println("[UPLOADER] timer tick")
				if err := w.processPendingUploads(ctx); err != nil {
					select {
					case w.errCh <- err:
					default:
						log.Printf("[UPLOADER[] worker error (no listener): %v", err)
					}
				}
			case <-w.notifyCh:
				log.Println("[UPLOADER] worker notified")
				_ = w.processPendingUploads(ctx)
			}
		}
	}()
}

func (w *UploadWorker) NotifyUploadQueued() {
	select {
	case w.notifyCh <- struct{}{}:
	default:
		// Don't block if notifyCh already has a signal
	}
}

func (w *UploadWorker) processPendingUploads(ctx context.Context) error {
	sem := make(chan struct{}, consts.MAX_CONCURRENCY)
	var wg sync.WaitGroup

	for {
		uploads, err := w.db.AcquireAndLeasePendingUploads(ctx, w.instanceID, consts.BATCH_SIZE)
		if err != nil {
			return fmt.Errorf("failed to list pending uploads: %w", err)
		}

		if len(uploads) == 0 {
			// Nothing to process, break and let the outer loop sleep
			break
		}

		for _, upload := range uploads {
			// Check if this upload has exceeded max attempts before processing
			if upload.Attempts >= consts.MAX_UPLOAD_ATTEMPTS {
				log.Printf("[UPLOADER] Skipping upload for hash %s (ID %d) due to excessive failed attempts (%d)", upload.ContentHash, upload.ID, upload.Attempts)
				continue // Skip this upload and move to the next one in the batch
			}

			select {
			case <-ctx.Done():
				log.Println("[UPLOADER] context cancelled")
				return nil
			case sem <- struct{}{}:
				wg.Add(1)
				go func(upload db.PendingUpload) {
					defer wg.Done()
					defer func() { <-sem }()
					w.processSingleUpload(ctx, upload)
				}(upload)
			}
		}
		wg.Wait()
	}
	return nil
}

func (w *UploadWorker) processSingleUpload(ctx context.Context, upload db.PendingUpload) {
	log.Printf("[UPLOADER] uploading hash %s", upload.ContentHash)

	// Check if this content hash is already marked as uploaded by another worker
	isUploaded, err := w.db.IsContentHashUploaded(ctx, upload.ContentHash)
	if err != nil {
		log.Printf("[UPLOADER] failed to check if content hash %s is already uploaded: %v", upload.ContentHash, err)
		// Mark attempt and let it be retried
		w.db.MarkUploadAttempt(ctx, upload.ContentHash) // Log error if this fails too?
		return
	}

	if isUploaded {
		log.Printf("[UPLOADER] content hash %s already uploaded, skipping S3 upload", upload.ContentHash)
		// Content is already in S3. Mark this specific message instance as uploaded
		// and delete the pending upload record.
		if err := w.db.CompleteS3Upload(ctx, upload.ContentHash); err != nil {
			log.Printf("[UPLOADER] - Warning: failed to finalize S3 upload for hash %s: %v", upload.ContentHash, err)
		} else {
			log.Printf("[UPLOADER] upload completed (already uploaded hash) for hash %s", upload.ContentHash)
		}
		// Still remove the local file as it's no longer needed on this instance
		filePath := w.FilePath(upload.ContentHash)
		if err := w.RemoveLocalFile(filePath); err != nil {
			log.Printf("[UPLOADER]- Warning: uploaded but could not delete file %s: %v", filePath, err)
		}
		return // Done with this upload record
	}

	filePath := w.FilePath(upload.ContentHash)
	data, err := os.ReadFile(filePath)
	if err != nil {
		w.db.MarkUploadAttempt(ctx, upload.ContentHash)
		log.Printf("[UPLOADER] could not read file %s: %v", filePath, err)
		return // Cannot proceed without the file
	}

	// Attempt to upload to S3. The storage layer should handle checking for existence.
	err = w.s3.Put(upload.ContentHash, bytes.NewReader(data), upload.Size)
	if err != nil {
		w.db.MarkUploadAttempt(ctx, upload.ContentHash)
		log.Printf("[UPLOADER] upload failed for %s: %v", upload.ContentHash, err)
		return
	}

	if upload.Size <= consts.MAX_CACHE_OBJECT_SIZE {
		if err := w.cache.MoveIn(filePath, upload.ContentHash); err != nil {
			log.Printf("[UPLOADER] failed to move uploaded hash %s to cache: %v", upload.ContentHash, err)
		} else {
			log.Printf("[UPLOADER] moved hash %s to cache after upload", upload.ContentHash)
		}
	} else {
		if err := w.RemoveLocalFile(filePath); err != nil {
			log.Printf("[UPLOADER]- Warning: uploaded but could not delete file %s: %v", filePath, err)
		}
	}

	err = w.db.CompleteS3Upload(ctx, upload.ContentHash)
	if err != nil {
		log.Printf("[UPLOADER] - Warning: failed to finalize S3 upload for hash %s: %v", upload.ContentHash, err)
	} else {
		log.Printf("[UPLOADER] upload completed for hash %s, ", upload.ContentHash)
	}
}

func (w *UploadWorker) FilePath(contentHash string) string {
	return filepath.Join(w.tempPath, contentHash)
}

func (w *UploadWorker) StoreLocally(contentHash string, data []byte) (*string, error) {
	path := w.FilePath(contentHash)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write file %s: %w", path, err)
	}
	return &path, nil
}

func (w *UploadWorker) RemoveLocalFile(path string) error {
	if err := os.Remove(path); err != nil {
		log.Printf("[UPLOADER] - Warning: uploaded but could not delete file %s: %v", path, err)
	} else {
		stopAt, _ := filepath.Abs(w.tempPath)
		removeEmptyParents(path, stopAt)
	}
	return nil
}

func removeEmptyParents(path, stopAt string) {
	for {
		parent := filepath.Dir(path)
		if parent == stopAt || parent == "." || parent == "/" {
			break
		}
		// Try removing the parent directory
		err := os.Remove(parent)
		if err != nil {
			// Stop if not empty or permission denied
			break
		}
		path = parent
	}
}

func (w *UploadWorker) GetLocalFile(contentHash string) ([]byte, error) {
	path := w.FilePath(contentHash)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read message %s from disk: %v", path, err)
	}
	return data, nil
}
