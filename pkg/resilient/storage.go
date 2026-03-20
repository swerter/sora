package resilient

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/migadu/sora/logger"
	"github.com/migadu/sora/pkg/circuitbreaker"
	"github.com/migadu/sora/pkg/retry"
	"github.com/migadu/sora/storage"
)

type ResilientS3Storage struct {
	storage       *storage.S3Storage
	getBreaker    *circuitbreaker.CircuitBreaker
	putBreaker    *circuitbreaker.CircuitBreaker
	deleteBreaker *circuitbreaker.CircuitBreaker
}

func NewResilientS3Storage(s3storage *storage.S3Storage) *ResilientS3Storage {
	getSettings := circuitbreaker.DefaultSettings("s3_get")
	getSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 5 && failureRatio >= 0.6
	}
	getSettings.OnStateChange = func(name string, from circuitbreaker.State, to circuitbreaker.State) {
		logger.Info("S3 GET circuit breaker changed", "name", name, "from", from, "to", to)
	}

	// isSuccessfulFunc prevents non-system errors from tripping the circuit breaker.
	// Only actual S3 infrastructure failures (5xx, timeouts, connection errors) should
	// count as failures. Client disconnects and S3 client errors (4xx) are not S3 outages.
	isSuccessfulFunc := func(err error) bool {
		if err == nil {
			return true
		}

		// Client disconnected (not an S3 failure).
		// Note: context.DeadlineExceeded is NOT excluded — if S3 is consistently
		// timing out, that IS a system health issue and should trip the breaker.
		if errors.Is(err, context.Canceled) {
			return true
		}

		// Use proper AWS SDK error typing for S3 client errors (4xx).
		// These are data/config issues, not S3 infrastructure failures.
		var httpErr *awshttp.ResponseError
		if errors.As(err, &httpErr) {
			code := httpErr.HTTPStatusCode()
			// 4xx errors are client errors (not found, access denied, etc.)
			// 5xx errors are server errors — should trip the breaker
			if code >= 400 && code < 500 {
				return true
			}
		}

		// Otherwise, it's considered a system failure (connection errors, 5xx, etc.)
		return false
	}

	getSettings.IsSuccessful = isSuccessfulFunc

	putSettings := circuitbreaker.DefaultSettings("s3_put")
	putSettings.IsSuccessful = isSuccessfulFunc
	putSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 3 && failureRatio >= 0.5
	}
	putSettings.OnStateChange = func(name string, from circuitbreaker.State, to circuitbreaker.State) {
		logger.Info("S3 PUT circuit breaker changed", "name", name, "from", from, "to", to)
	}

	deleteSettings := circuitbreaker.DefaultSettings("s3_delete")
	deleteSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 3 && failureRatio >= 0.5
	}
	deleteSettings.OnStateChange = func(name string, from circuitbreaker.State, to circuitbreaker.State) {
		logger.Info("S3 DELETE circuit breaker changed", "name", name, "from", from, "to", to)
	}
	deleteSettings.IsSuccessful = isSuccessfulFunc

	return &ResilientS3Storage{
		storage:       s3storage,
		getBreaker:    circuitbreaker.NewCircuitBreaker(getSettings),
		putBreaker:    circuitbreaker.NewCircuitBreaker(putSettings),
		deleteBreaker: circuitbreaker.NewCircuitBreaker(deleteSettings),
	}
}

func (rs *ResilientS3Storage) GetStorage() *storage.S3Storage {
	return rs.storage
}

func (rs *ResilientS3Storage) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	retryableErrors := []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"i/o timeout",
		"network unreachable",
		"no such host",
		"temporary failure",
		"service unavailable",
		"internal server error",
		"bad gateway",
		"gateway timeout",
		"timeout",
		"slowdown",
		"throttling",
		"rate limit",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, retryable) {
			return true
		}
	}

	return false
}

func (rs *ResilientS3Storage) GetWithRetry(ctx context.Context, key string) (io.ReadCloser, error) {
	config := retry.BackoffConfig{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      4,
		OperationName:   "s3_get",
	}

	op := func() (any, error) {
		return rs.storage.Get(key)
	}
	result, err := rs.executeS3OperationWithRetry(ctx, rs.getBreaker, config, op)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return result.(io.ReadCloser), nil
}

func (rs *ResilientS3Storage) PutWithRetry(ctx context.Context, key string, body io.Reader, size int64) error {
	config := retry.BackoffConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      3,
		OperationName:   "s3_put",
	}

	op := func() (any, error) {
		return nil, rs.storage.Put(key, body, size)
	}
	_, err := rs.executeS3OperationWithRetry(ctx, rs.putBreaker, config, op)
	return err
}

// IsHealthy returns true if S3 circuit breakers are not open (S3 is reachable).
// Used by the cleaner to skip destructive operations when S3 is down.
func (rs *ResilientS3Storage) IsHealthy() bool {
	return rs.putBreaker.State() != circuitbreaker.StateOpen &&
		rs.getBreaker.State() != circuitbreaker.StateOpen
}

func (rs *ResilientS3Storage) DeleteWithRetry(ctx context.Context, key string) error {
	config := retry.BackoffConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     15 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      3,
		OperationName:   "s3_delete",
	}

	op := func() (any, error) {
		return nil, rs.storage.Delete(key)
	}
	_, err := rs.executeS3OperationWithRetry(ctx, rs.deleteBreaker, config, op)
	return err
}

func (rs *ResilientS3Storage) PutObjectWithRetry(ctx context.Context, key string, reader io.Reader, objectSize int64) (*s3.PutObjectOutput, error) {
	config := retry.BackoffConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      3,
		OperationName:   "s3_put_object",
	}

	op := func() (any, error) {
		input := &s3.PutObjectInput{
			Bucket: aws.String(rs.storage.BucketName),
			Key:    aws.String(key),
			Body:   reader,
		}
		return rs.storage.Client.PutObject(ctx, input)
	}
	result, err := rs.executeS3OperationWithRetry(ctx, rs.putBreaker, config, op)
	if err != nil {
		return nil, err
	}
	output := result.(*s3.PutObjectOutput)
	return output, err
}

func (rs *ResilientS3Storage) GetObjectWithRetry(ctx context.Context, key string) (*s3.GetObjectOutput, error) {
	config := retry.BackoffConfig{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      4,
		OperationName:   "s3_get_object",
	}

	op := func() (any, error) {
		input := &s3.GetObjectInput{
			Bucket: aws.String(rs.storage.BucketName),
			Key:    aws.String(key),
		}
		return rs.storage.Client.GetObject(ctx, input)
	}
	result, err := rs.executeS3OperationWithRetry(ctx, rs.getBreaker, config, op)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	output := result.(*s3.GetObjectOutput)
	return output, err
}

// ExistsWithRetry returns true if an S3 object with the given key exists.
// A 404 response is treated as "does not exist" (returns false, nil).
// All other errors are returned as-is.
// This is used by the uploader to self-heal stuck uploads whose local file
// was deleted but whose content was already successfully stored in S3.
func (rs *ResilientS3Storage) ExistsWithRetry(ctx context.Context, key string) (bool, error) {
	_, err := rs.StatObjectWithRetry(ctx, key)
	if err != nil {
		// A 404 means the object simply isn't there — not an error condition.
		var httpErr *awshttp.ResponseError
		if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (rs *ResilientS3Storage) StatObjectWithRetry(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	config := retry.BackoffConfig{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     5 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
		MaxRetries:      3,
		OperationName:   "s3_stat_object",
	}

	op := func() (any, error) {
		input := &s3.HeadObjectInput{
			Bucket: aws.String(rs.storage.BucketName),
			Key:    aws.String(key),
		}
		return rs.storage.Client.HeadObject(ctx, input)
	}
	result, err := rs.executeS3OperationWithRetry(ctx, rs.getBreaker, config, op)
	if err != nil {
		return nil, err
	}
	output := result.(*s3.HeadObjectOutput)
	return output, err
}

// executeS3OperationWithRetry provides a generic wrapper for executing an S3 operation with retries and a circuit breaker.
func (rs *ResilientS3Storage) executeS3OperationWithRetry(ctx context.Context, breaker *circuitbreaker.CircuitBreaker, config retry.BackoffConfig, op func() (any, error)) (any, error) {
	var result any
	err := retry.WithRetryAdvanced(ctx, func() error {
		res, cbErr := breaker.Execute(op)
		if cbErr != nil {
			// Log every S3 failure so we can diagnose what triggers circuit breaker trips.
			// Without this, tripped breakers are invisible until users report empty bodies.
			logger.Warn("S3 operation failed",
				"breaker", breaker.Name(),
				"error", cbErr,
				"breaker_state", breaker.State(),
				"retryable", rs.isRetryableError(cbErr))

			if rs.isRetryableError(cbErr) {
				return cbErr // Signal to retry
			}
			// Use retry.Stop for non-retryable errors (e.g. circuit breaker open)
			// to stop the loop immediately. WithRetryAdvanced respects StopError.
			return retry.Stop(cbErr)
		}
		result = res
		return nil
	}, config)
	return result, err
}

func (rs *ResilientS3Storage) GetGetBreakerState() circuitbreaker.State {
	return rs.getBreaker.State()
}

func (rs *ResilientS3Storage) GetPutBreakerState() circuitbreaker.State {
	return rs.putBreaker.State()
}

func (rs *ResilientS3Storage) GetDeleteBreakerState() circuitbreaker.State {
	return rs.deleteBreaker.State()
}
