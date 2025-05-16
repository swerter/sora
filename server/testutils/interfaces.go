package testutils

import (
	"io"
)

// S3StorageInterface defines the interface for S3 storage operations
type S3StorageInterface interface {
	Get(key string) (io.ReadCloser, error)
	Put(key string, body io.Reader, size int64) error
	Delete(key string) error
	Exists(key string) (bool, string, error)
	Copy(sourcePath, destPath string) error
}

// UploadWorkerInterface defines the interface for upload worker operations
type UploadWorkerInterface interface {
	GetLocalFile(contentHash string) ([]byte, error)
	StoreLocally(contentHash string, data []byte) (*string, error)
	RemoveLocalFile(path string) error
	FilePath(contentHash string) string
	NotifyUploadQueued()
}

// CacheInterface defines the interface for cache operations
type CacheInterface interface {
	Get(contentHash string) ([]byte, error)
	Put(contentHash string, data []byte) error
	Delete(contentHash string) error
}
