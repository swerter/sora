package resilient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/migadu/sora/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutWithRetryRewindsReader tests that PutWithRetry rewinds the io.Reader 
// when retrying after a transient error, preventing 0-byte uploads.
func TestPutWithRetryRewindsReader(t *testing.T) {
	// A mock S3 server that fails on the first PUT request, and succeeds on the second.
	var requestCount int32
	var finalBodyRead []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			count := atomic.AddInt32(&requestCount, 1)

			// Read the body completely (simulating the S3 SDK consuming the body)
			body, _ := io.ReadAll(r.Body)

			if count == 1 {
				// First attempt: simulate a transient S3 error (HTTP 500)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Second attempt: store the body and succeed
			finalBodyRead = body
			w.WriteHeader(http.StatusOK)
			return
		}
		
		// For any other requests, just return 200 (like HEAD for existence check etc if needed)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Parse out just the host:port for the storage.New endpoint
	endpoint := strings.TrimPrefix(server.URL, "http://")

	// Create real storage client pointing to our mock server
	s3storage, err := storage.New(
		endpoint,
		"test-access-key",
		"test-secret-key",
		"test-bucket",
		false, // useSSL=false
		false, // debug=false
		5*time.Second,
	)
	require.NoError(t, err)

	// Create resilient wrapper
	resilientS3 := NewResilientS3Storage(s3storage)

	// The data we want to upload
	expectedData := []byte("hello world, this is a test payload for S3 upload")
	reader := bytes.NewReader(expectedData)

	// Ensure the breaker doesn't trip on the first 500 (default settings: min 3 requests)
	ctx := context.Background()
	key := "test-object-key"
	size := int64(len(expectedData))

	// The PutWithRetry should fail once, rewind the reader, retry, and succeed.
	err = resilientS3.PutWithRetry(ctx, key, reader, size)
	
	// Expectations
	require.NoError(t, err, "PutWithRetry should eventually succeed")
	assert.Equal(t, int32(2), atomic.LoadInt32(&requestCount), "Should have retried exactly once")
	assert.Equal(t, expectedData, finalBodyRead, "The second S3 attempt should have received the FULL data because the reader was rewound")
}
