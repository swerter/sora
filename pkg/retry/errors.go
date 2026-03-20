package retry

import "errors"

// Sentinel errors for retry operations.
// These errors can be checked using errors.Is() for proper error handling.

var (
	// ErrMaxRetriesExceeded indicates that an operation failed after all retry attempts
	ErrMaxRetriesExceeded = errors.New("operation failed after maximum retries")
)
