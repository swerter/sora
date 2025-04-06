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

	"github.com/google/uuid"
	"github.com/migadu/sora/cache"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/storage"
)

type UploadWorker struct {
	db       *db.Database
	s3       *storage.S3Storage
	cache    *cache.Cache
	tempPath string
	notifyCh chan struct{}
	errCh    chan<- error
}

func New(ctx context.Context, tempPath string, db *db.Database, s3 *storage.S3Storage, cache *cache.Cache, errCh chan<- error) (*UploadWorker, error) {
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		if err := os.MkdirAll(tempPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create local path %s: %w", tempPath, err)
		}
	}
	notifyCh := make(chan struct{}, 1)

	return &UploadWorker{
		db:       db,
		s3:       s3,
		cache:    cache,
		errCh:    errCh,
		tempPath: tempPath,
		notifyCh: notifyCh,
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
				log.Println("Upload worker stopped")
				return
			case <-ticker.C:
				log.Println("Upload worker timer tick -----------------")
				if err := w.processPendingUploads(ctx); err != nil {
					select {
					case w.errCh <- err:
					default:
						log.Printf("Upload worker error (no listener): %v", err)
					}
				}
			case <-w.notifyCh:
				log.Println("Upload worker notified -----------------")
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
		uploads, err := w.db.ListPendingUploads(ctx, consts.BATCH_SIZE)
		if err != nil {
			return fmt.Errorf("failed to list pending uploads: %w", err)
		}

		if len(uploads) == 0 {
			// Nothing to process, break and let the outer loop sleep
			break
		}

		for _, upload := range uploads {
			select {
			case <-ctx.Done():
				log.Println("Upload context cancelled")
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
	log.Printf("Retrying S3 upload for message %d (%s)", upload.MessageID, upload.UUID)

	data, err := os.ReadFile(upload.FilePath)
	if err != nil {
		w.db.MarkUploadAttempt(ctx, upload.ID, false)
		log.Printf("Could not read file %s: %v", upload.FilePath, err)
		return
	}

	err = w.s3.SaveMessage(upload.S3Path, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		w.db.MarkUploadAttempt(ctx, upload.ID, false)
		log.Printf("S3 upload failed for %s: %v", upload.S3Path, err)
		return
	}

	if upload.Size == 0 {
		log.Fatal("Warning: upload size is 0")
		return
	}

	if upload.Size <= cache.MAX_MESSAGE {
		if err := w.cache.MoveIn(upload.FilePath, upload.DomainName, upload.LocalPart, upload.UUID); err != nil {
			log.Printf("Failed to move uploaded message %d to cache: %v", upload.MessageID, err)
		} else {
			log.Printf("Moved message %d to cache after upload", upload.MessageID)
		}
	} else {
		if err := w.RemoveLocalFile(upload.FilePath); err != nil {
			log.Printf("Warning: uploaded but could not delete file %s: %v", upload.FilePath, err)
		}
	}

	err = w.db.CompleteS3Upload(ctx, upload.MessageID, upload.ID)
	if err != nil {
		log.Printf("Warning: failed to finalize S3 upload for message %d: %v", upload.MessageID, err)
	} else {
		log.Printf("Upload finalized for message %d", upload.MessageID)
	}
}

func (w *UploadWorker) FilePath(domain, localPart string, uuid uuid.UUID) string {
	return filepath.Join(w.tempPath, domain, localPart, uuid.String())
}

func (w *UploadWorker) StoreLocally(domain, localPart string, uuid uuid.UUID, data []byte) (*string, error) {
	path := w.FilePath(domain, localPart, uuid)
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
		log.Printf("Warning: uploaded but could not delete file %s: %v", path, err)
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

func (w *UploadWorker) GetLocalFile(domain, localPart string, uuid uuid.UUID) ([]byte, error) {
	path := w.FilePath(domain, localPart, uuid)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read message %s from disk: %v", path, err)
	}
	return data, nil
}
