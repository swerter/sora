package uploader

import (
	"context"
	"fmt"
	"testing"

	"github.com/migadu/sora/pkg/circuitbreaker"
)

func TestIsTransientS3Error(t *testing.T) {
	w := &UploadWorker{}

	// Test sentinel errors (typed errors)
	t.Run("sentinel errors", func(t *testing.T) {
		sentinelErrors := []error{
			circuitbreaker.ErrCircuitBreakerOpen,
			circuitbreaker.ErrTooManyRequests,
			context.DeadlineExceeded,
			context.Canceled,
		}

		for _, err := range sentinelErrors {
			if !w.isTransientS3Error(err) {
				t.Errorf("Expected transient for sentinel error %v, got permanent", err)
			}
		}
	})

	// Test string-based errors (for external libraries like AWS SDK)
	t.Run("string-based errors", func(t *testing.T) {
		transientErrors := []string{
			"connection refused",
			"connection reset by peer",
			"i/o timeout",
			"network unreachable",
			"service unavailable",
			"bad gateway",
			"gateway timeout",
			"rate limit exceeded",
			"request throttling",
			"SlowDown: Please reduce your request rate",
		}

		for _, errStr := range transientErrors {
			err := fmt.Errorf("%s", errStr)
			if !w.isTransientS3Error(err) {
				t.Errorf("Expected transient for %q, got permanent", errStr)
			}
		}
	})

	permanentErrors := []string{
		"access denied",
		"no such key",
		"invalid argument",
		"bucket not found",
		"signature mismatch",
	}

	for _, errStr := range permanentErrors {
		err := fmt.Errorf("%s", errStr)
		if w.isTransientS3Error(err) {
			t.Errorf("Expected permanent for %q, got transient", errStr)
		}
	}

	// nil error
	if w.isTransientS3Error(nil) {
		t.Error("Expected false for nil error")
	}

	t.Log("✓ isTransientS3Error correctly classifies transient vs permanent S3 errors")
}
