package storage

import "errors"

// Sentinel errors for storage operations.
// These errors can be checked using errors.Is() for proper error handling.

var (
	// ErrRetrieveFailed indicates that retrieval from storage failed
	ErrRetrieveFailed = errors.New("failed to retrieve message from storage")

	// ErrEmptyData indicates that storage returned empty data
	ErrEmptyData = errors.New("storage returned empty data")
)
