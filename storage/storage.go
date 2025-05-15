// storage/s3_storage.go
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Storage struct {
	Client     *minio.Client
	BucketName string
}

func New(endpoint, accessKeyID, secretAccessKey, bucketName string, useSSL bool) (*S3Storage, error) {
	// Initialize the MinIO client
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL, // Use SSL (https) if true
	})
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	// Enable detailed tracing of requests and responses for debugging
	client.TraceOn(os.Stdout)

	// Return the initialized storage client
	return &S3Storage{
		Client:     client,
		BucketName: bucketName,
	}, nil
}

// Exists checks if an object with the given key exists in the bucket.
func (s *S3Storage) Exists(key string) (bool, string, error) {
	objInfo, err := s.Client.StatObject(context.Background(), s.BucketName, key, minio.StatObjectOptions{})
	if err == nil {
		return true, objInfo.VersionID, nil // Object exists
	}

	// Check if the error is a minio.ErrorResponse
	var minioErr minio.ErrorResponse
	if errors.As(err, &minioErr) {
		if minioErr.StatusCode == 404 {
			return false, "", nil // Object does not exist
		}
	}

	// Other error occurred
	return false, "", fmt.Errorf("failed to stat object %s: %w", key, err)
}

func (s *S3Storage) Put(key string, body io.Reader, size int64) error {
	exists, _, err := s.Exists(key)
	if err != nil {
		log.Printf("Error checking existence of object %s: %v", key, err)
		return err
	}
	if exists {
		log.Printf("Object %s already exists in S3, skipping upload.", key)
		return nil // Object already exists, no need to upload
	}

	_, err = s.Client.PutObject(context.Background(), s.BucketName, key, body, size, minio.PutObjectOptions{SendContentMd5: true})
	return err
}

func (s *S3Storage) Get(key string) (io.ReadCloser, error) {
	object, err := s.Client.GetObject(context.Background(), s.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (s *S3Storage) Delete(key string) error {
	// Check if the object exists before attempting to delete.
	// This makes DeleteMessage idempotent.
	exists, versionId, err := s.Exists(key)
	if err != nil {
		log.Printf("Error checking existence of object %s: %v", key, err)
		return err
	}
	if !exists {
		// Object does not exist, consider it successfully "deleted"
		log.Printf("Object %s does not exist in S3, skipping deletion.", key)
		return nil
	}
	return s.Client.RemoveObject(context.Background(), s.BucketName, key, minio.RemoveObjectOptions{VersionID: versionId})
}

func (s *S3Storage) Copy(sourcePath, destPath string) error {
	src := minio.CopySrcOptions{
		Bucket: s.BucketName,
		Object: sourcePath,
	}
	dst := minio.CopyDestOptions{
		Bucket: s.BucketName,
		Object: destPath,
	}
	_, err := s.Client.CopyObject(context.Background(), dst, src)
	if err != nil {
		return fmt.Errorf("failed to copy object from %s to %s: %v", sourcePath, destPath, err)
	}
	return nil
}
