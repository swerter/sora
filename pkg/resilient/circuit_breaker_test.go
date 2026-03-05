package resilient

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/pkg/circuitbreaker"
)

// TestCircuitBreakerBusinessLogicErrors verifies that business logic errors
// (like user not found, invalid password) do NOT trip the circuit breaker.
// Only actual system failures should trip the circuit breaker.
func TestCircuitBreakerBusinessLogicErrors(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		shouldSucceed bool // Should this error be treated as success by circuit breaker?
	}{
		{
			name:          "nil error is success",
			err:           nil,
			shouldSucceed: true,
		},
		{
			name:          "user not found is success (business logic)",
			err:           consts.ErrUserNotFound,
			shouldSucceed: true,
		},
		{
			name:          "mailbox not found is success (business logic)",
			err:           consts.ErrMailboxNotFound,
			shouldSucceed: true,
		},
		{
			name:          "mailbox already exists is success (business logic)",
			err:           consts.ErrMailboxAlreadyExists,
			shouldSucceed: true,
		},
		{
			name:          "account already exists is success (business logic)",
			err:           consts.ErrAccountAlreadyExists,
			shouldSucceed: true,
		},
		{
			name:          "message not available is success (business logic)",
			err:           consts.ErrMessageNotAvailable,
			shouldSucceed: true,
		},
		{
			name:          "not permitted is success (business logic)",
			err:           consts.ErrNotPermitted,
			shouldSucceed: true,
		},
		{
			name:          "pgx.ErrNoRows is success (business logic)",
			err:           pgx.ErrNoRows,
			shouldSucceed: true,
		},
		{
			name:          "unique violation is success for writes (business logic)",
			err:           consts.ErrDBUniqueViolation,
			shouldSucceed: true, // Only for write breaker
		},
		{
			name:          "wrapped user not found is success",
			err:           errors.Join(errors.New("auth failed"), consts.ErrUserNotFound),
			shouldSucceed: true,
		},
		{
			name:          "wrapped account already exists is success (formatted error)",
			err:           errors.New("account with email user@example.com already exists: " + consts.ErrAccountAlreadyExists.Error()),
			shouldSucceed: false, // This won't match because it's not wrapped with %w
		},
		{
			name:          "properly wrapped account already exists is success",
			err:           fmt.Errorf("%w: account with email user@example.com already exists", consts.ErrAccountAlreadyExists),
			shouldSucceed: true, // This will match because it uses %w
		},
		{
			name:          "generic error is failure (system error)",
			err:           errors.New("database connection failed"),
			shouldSucceed: false,
		},
		{
			name:          "context deadline exceeded is failure (system error)",
			err:           context.DeadlineExceeded,
			shouldSucceed: false,
		},
		{
			name:          "internal error is failure (system error)",
			err:           consts.ErrInternalError,
			shouldSucceed: false,
		},
	}

	// Test query circuit breaker
	t.Run("QueryCircuitBreaker", func(t *testing.T) {
		for _, tt := range tests {
			// Skip unique violation test for query breaker (write-only)
			if tt.name == "unique violation is success for writes (business logic)" {
				continue
			}

			t.Run(tt.name, func(t *testing.T) {
				// Create fresh circuit breaker for each test case
				querySettings := circuitbreaker.DefaultSettings("test_query")
				querySettings.MaxRequests = 5
				querySettings.Interval = 0
				querySettings.Timeout = 0
				querySettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
					// Trip after 3 failures
					return counts.TotalFailures >= 3
				}

				// Configure IsSuccessful to match production settings
				querySettings.IsSuccessful = func(err error) bool {
					if err == nil {
						return true
					}
					if errors.Is(err, consts.ErrUserNotFound) ||
						errors.Is(err, consts.ErrMailboxNotFound) ||
						errors.Is(err, consts.ErrMessageNotAvailable) ||
						errors.Is(err, consts.ErrMailboxAlreadyExists) ||
						errors.Is(err, consts.ErrAccountAlreadyExists) ||
						errors.Is(err, consts.ErrNotPermitted) ||
						errors.Is(err, pgx.ErrNoRows) {
						return true
					}
					return false
				}

				breaker := circuitbreaker.NewCircuitBreaker(querySettings)

				// Execute operation through circuit breaker
				_, err := breaker.Execute(func() (any, error) {
					return nil, tt.err
				})

				// Get circuit breaker counts
				counts := breaker.Counts()

				if tt.shouldSucceed {
					// Business logic errors should be counted as successes
					if counts.TotalFailures > 0 {
						t.Errorf("Expected business logic error to not increment failure count, but got %d failures", counts.TotalFailures)
					}
					if counts.TotalSuccesses != 1 {
						t.Errorf("Expected business logic error to increment success count to 1, but got %d successes", counts.TotalSuccesses)
					}
					// Verify circuit breaker remains closed
					if breaker.State() != circuitbreaker.StateClosed {
						t.Errorf("Circuit breaker should remain CLOSED after business logic error, but state is: %s", breaker.State())
					}
				} else {
					// System errors should be counted as failures
					if counts.TotalFailures != 1 {
						t.Errorf("Expected system error to increment failure count to 1, but got %d failures", counts.TotalFailures)
					}
				}

				// Verify the returned error matches what we passed in
				if tt.err == nil && err != nil {
					t.Errorf("Expected nil error, got: %v", err)
				}
				if tt.err != nil && err == nil {
					t.Errorf("Expected error %v, got nil", tt.err)
				}
			})
		}
	})

	// Test write circuit breaker (includes unique violation)
	t.Run("WriteCircuitBreaker", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Create fresh circuit breaker for each test case
				writeSettings := circuitbreaker.DefaultSettings("test_write")
				writeSettings.MaxRequests = 3
				writeSettings.Interval = 0
				writeSettings.Timeout = 0
				writeSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
					return counts.TotalFailures >= 3
				}

				// Configure IsSuccessful to match production settings (includes unique violation)
				writeSettings.IsSuccessful = func(err error) bool {
					if err == nil {
						return true
					}
					if errors.Is(err, consts.ErrUserNotFound) ||
						errors.Is(err, consts.ErrMailboxNotFound) ||
						errors.Is(err, consts.ErrMessageNotAvailable) ||
						errors.Is(err, consts.ErrMailboxAlreadyExists) ||
						errors.Is(err, consts.ErrAccountAlreadyExists) ||
						errors.Is(err, consts.ErrNotPermitted) ||
						errors.Is(err, consts.ErrDBUniqueViolation) ||
						errors.Is(err, pgx.ErrNoRows) {
						return true
					}
					return false
				}

				breaker := circuitbreaker.NewCircuitBreaker(writeSettings)

				_, execErr := breaker.Execute(func() (any, error) {
					return nil, tt.err
				})

				counts := breaker.Counts()

				// Verify the returned error matches expectations
				_ = execErr // Silence unused variable warning

				if tt.shouldSucceed {
					if counts.TotalFailures > 0 {
						t.Errorf("Expected business logic error to not increment failure count, but got %d failures", counts.TotalFailures)
					}
					if counts.TotalSuccesses != 1 {
						t.Errorf("Expected business logic error to increment success count to 1, but got %d successes", counts.TotalSuccesses)
					}
					// Verify circuit breaker remains closed
					if breaker.State() != circuitbreaker.StateClosed {
						t.Errorf("Circuit breaker should remain CLOSED after business logic error, but state is: %s", breaker.State())
					}
				} else {
					if counts.TotalFailures != 1 {
						t.Errorf("Expected system error to increment failure count to 1, but got %d failures", counts.TotalFailures)
					}
				}
			})
		}
	})
}

// TestCircuitBreakerAuthenticationFailures simulates a burst of authentication
// failures (user not found) and verifies the circuit breaker remains closed.
func TestCircuitBreakerAuthenticationFailures(t *testing.T) {
	querySettings := circuitbreaker.DefaultSettings("test_auth")
	querySettings.MaxRequests = 5
	querySettings.Interval = 0
	querySettings.Timeout = 0
	querySettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		// Trip after 60% failure rate with at least 8 requests
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 8 && failureRatio >= 0.6
	}

	// Configure IsSuccessful to match production settings
	querySettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		// User not found is a business logic error, not a system failure
		if errors.Is(err, consts.ErrUserNotFound) ||
			errors.Is(err, pgx.ErrNoRows) {
			return true
		}
		return false
	}

	breaker := circuitbreaker.NewCircuitBreaker(querySettings)

	// Simulate 20 consecutive authentication failures (user not found)
	for i := 0; i < 20; i++ {
		_, err := breaker.Execute(func() (any, error) {
			// Simulate database query returning "user not found"
			return nil, consts.ErrUserNotFound
		})

		// The error should be returned to the caller
		if !errors.Is(err, consts.ErrUserNotFound) {
			t.Fatalf("Expected ErrUserNotFound, got: %v", err)
		}
	}

	// Verify circuit breaker counts
	counts := breaker.Counts()
	if counts.Requests != 20 {
		t.Errorf("Expected 20 requests, got %d", counts.Requests)
	}
	if counts.TotalSuccesses != 20 {
		t.Errorf("Expected 20 successes (business logic errors), got %d", counts.TotalSuccesses)
	}
	if counts.TotalFailures != 0 {
		t.Errorf("Expected 0 failures (user not found should not count), got %d", counts.TotalFailures)
	}

	// Verify circuit breaker remains CLOSED
	if breaker.State() != circuitbreaker.StateClosed {
		t.Errorf("Circuit breaker should remain CLOSED after authentication failures, but state is: %s", breaker.State())
	}
}

// TestCircuitBreakerSystemFailures verifies that actual system failures
// DO trip the circuit breaker.
func TestCircuitBreakerSystemFailures(t *testing.T) {
	querySettings := circuitbreaker.DefaultSettings("test_system_failures")
	querySettings.MaxRequests = 5
	querySettings.Interval = 0
	querySettings.Timeout = 0
	querySettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		// Trip after 3 system failures
		return counts.TotalFailures >= 3
	}

	querySettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		// Only business logic errors are successes
		if errors.Is(err, consts.ErrUserNotFound) {
			return true
		}
		return false
	}

	breaker := circuitbreaker.NewCircuitBreaker(querySettings)

	// Simulate 3 system failures (database connection errors)
	systemError := errors.New("database connection failed")
	for i := 0; i < 2; i++ {
		_, execErr := breaker.Execute(func() (any, error) {
			return nil, systemError
		})

		if execErr == nil {
			t.Fatalf("Expected system error, got nil")
		}

		// Check counts after each failure (before circuit breaker opens)
		counts := breaker.Counts()
		if counts.TotalFailures != uint32(i+1) {
			t.Errorf("After failure %d, expected %d failures, got %d", i+1, i+1, counts.TotalFailures)
		}

		// Circuit breaker should still be closed
		if breaker.State() != circuitbreaker.StateClosed {
			t.Errorf("Circuit breaker should be CLOSED after only %d failures, but state is: %s", i+1, breaker.State())
		}
	}

	// Execute third failure which should open the circuit
	_, execErr := breaker.Execute(func() (any, error) {
		return nil, systemError
	})

	if execErr == nil {
		t.Fatalf("Expected system error, got nil")
	}

	// Verify circuit breaker OPENED after 3 system failures
	if breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("Circuit breaker should be OPEN after 3 system failures, but state is: %s", breaker.State())
	}

	// Verify subsequent requests are rejected with circuit breaker error
	_, err := breaker.Execute(func() (any, error) {
		return nil, nil
	})

	if !errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
		t.Errorf("Expected ErrCircuitBreakerOpen, got: %v", err)
	}
}

// TestCircuitBreakerMixedErrors verifies that business logic errors don't
// interfere with detection of system failures.
func TestCircuitBreakerMixedErrors(t *testing.T) {
	querySettings := circuitbreaker.DefaultSettings("test_mixed")
	querySettings.MaxRequests = 5
	querySettings.Interval = 0
	querySettings.Timeout = 0
	querySettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 10 && failureRatio >= 0.6
	}

	querySettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		if errors.Is(err, consts.ErrUserNotFound) {
			return true
		}
		return false
	}

	breaker := circuitbreaker.NewCircuitBreaker(querySettings)

	// Mix of business logic errors and system failures
	testErrors := []error{
		consts.ErrUserNotFound, // business logic
		consts.ErrUserNotFound, // business logic
		errors.New("timeout"),  // system failure
		consts.ErrUserNotFound, // business logic
		errors.New("timeout"),  // system failure
		consts.ErrUserNotFound, // business logic
		errors.New("timeout"),  // system failure
		consts.ErrUserNotFound, // business logic
		errors.New("timeout"),  // system failure
		errors.New("timeout"),  // system failure
		errors.New("timeout"),  // system failure
		errors.New("timeout"),  // system failure
	}

	var successCount, failureCount int64
	for _, testErr := range testErrors {
		breaker.Execute(func() (any, error) {
			return nil, testErr
		})

		// Track counts as we go
		if errors.Is(testErr, consts.ErrUserNotFound) {
			successCount++
		} else {
			failureCount++
		}
	}

	counts := breaker.Counts()
	// 5 business logic errors (user not found) = 5 successes
	// 7 system failures = 7 failures
	if counts.TotalSuccesses != 5 {
		t.Errorf("Expected 5 successes, got %d", counts.TotalSuccesses)
	}
	if counts.TotalFailures != 7 {
		t.Errorf("Expected 7 failures, got %d", counts.TotalFailures)
	}

	// Circuit breaker should remain CLOSED: 12 requests, 7 failures = 58% failure ratio (below 60% threshold)
	if breaker.State() != circuitbreaker.StateClosed {
		t.Errorf("Circuit breaker should remain CLOSED (failure ratio = 7/12 = 58%% < 60%% threshold), but state is: %s", breaker.State())
	}

	// Add one more system failure to cross the 60% threshold
	breaker.Execute(func() (any, error) {
		return nil, errors.New("timeout")
	})

	// Now: 13 requests, 8 failures = 61.5% failure ratio
	// Circuit breaker should open at this point (61.5% > 60%)
	if breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("Circuit breaker should be OPEN (failure ratio = 8/13 = 61.5%% > 60%% threshold), but state is: %s", breaker.State())
	}
}

// TestCircuitBreakerHalfOpenDeadlock tests the scenario where the circuit breaker
// could get stuck in HALF_OPEN state forever. This was the bug that caused 4-hour outages.
//
// The deadlock occurred when:
// 1. Circuit breaker went to HALF_OPEN
// 2. MaxRequests = 3 (only 3 allowed through)
// 3. All 3 requests failed
// 4. ReadyToTrip required >= 5 requests
// 5. Circuit couldn't transition to OPEN (3 < 5)
// 6. Circuit stuck in HALF_OPEN forever
func TestCircuitBreakerHalfOpenDeadlock(t *testing.T) {
	t.Run("OldBehavior_WithBusinessLogicErrors", func(t *testing.T) {
		// Simulate OLD behavior (before fix):
		// - MaxRequests = 3
		// - ReadyToTrip requires >= 5 requests
		// This would cause DEADLOCK

		settings := circuitbreaker.DefaultSettings("test_deadlock_old")
		settings.MaxRequests = 3 // OLD: Only 3 requests in half-open
		settings.Interval = 0
		settings.Timeout = 100 * time.Millisecond // Short timeout for testing
		settings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
			// OLD: Required >= 5 requests
			// This creates deadlock: can't get 5 when only 3 allowed!
			return counts.Requests >= 5 && float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		}

		// Treat business logic errors as FAILURES (old buggy behavior)
		settings.IsSuccessful = func(err error) bool {
			return err == nil
		}

		breaker := circuitbreaker.NewCircuitBreaker(settings)

		// Force circuit to OPEN by causing failures
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) {
				return nil, errors.New("system failure")
			})
		}

		if breaker.State() != circuitbreaker.StateOpen {
			t.Fatalf("Expected OPEN state, got %s", breaker.State())
		}

		// Wait for timeout to transition to HALF_OPEN
		time.Sleep(150 * time.Millisecond)

		// Trigger state check by attempting a request
		breaker.Execute(func() (any, error) {
			return nil, consts.ErrUserNotFound // Business logic error
		})

		state := breaker.State()
		if state != circuitbreaker.StateHalfOpen {
			t.Fatalf("Expected HALF_OPEN state after timeout, got %s", state)
		}

		// Now simulate 3 business logic errors (which are counted as failures in old behavior)
		for i := 0; i < 2; i++ { // 2 more (we already did 1)
			breaker.Execute(func() (any, error) {
				return nil, consts.ErrUserNotFound
			})
		}

		// OLD BEHAVIOR: Circuit breaker stays in HALF_OPEN
		// Because: 3 requests, all failed, but ReadyToTrip needs >= 5
		counts := breaker.Counts()
		state = breaker.State()

		t.Logf("After 3 failures in half-open: state=%s, requests=%d, failures=%d",
			state, counts.Requests, counts.TotalFailures)

		// This demonstrates the deadlock: circuit is HALF_OPEN but can't transition
		if state == circuitbreaker.StateHalfOpen && counts.Requests >= settings.MaxRequests {
			t.Logf("DEADLOCK SCENARIO: Circuit stuck in HALF_OPEN (requests=%d < required=5 for ReadyToTrip)", counts.Requests)
			// In production, this state would persist forever, rejecting all requests
		}
	})

	t.Run("NewBehavior_WithBusinessLogicErrors", func(t *testing.T) {
		// Simulate NEW behavior (after fix):
		// - MaxRequests = 10
		// - ReadyToTrip checks if half-open exhausted (>= MaxRequests)
		// - Business logic errors counted as SUCCESS
		// This PREVENTS deadlock

		settings := circuitbreaker.DefaultSettings("test_deadlock_new")
		settings.MaxRequests = 10 // NEW: More requests in half-open
		settings.Interval = 0
		settings.Timeout = 100 * time.Millisecond
		settings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)

			// NEW: Check if half-open exhausted
			if counts.Requests >= settings.MaxRequests && failureRatio >= 0.5 {
				return true
			}

			// Normal operation
			return counts.Requests >= 5 && failureRatio >= 0.5
		}

		// NEW: Treat business logic errors as SUCCESS
		settings.IsSuccessful = func(err error) bool {
			if err == nil {
				return true
			}
			return errors.Is(err, consts.ErrUserNotFound)
		}

		breaker := circuitbreaker.NewCircuitBreaker(settings)

		// Force circuit to OPEN
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) {
				return nil, errors.New("system failure")
			})
		}

		if breaker.State() != circuitbreaker.StateOpen {
			t.Fatalf("Expected OPEN state, got %s", breaker.State())
		}

		// Wait for timeout
		time.Sleep(150 * time.Millisecond)

		// Trigger transition to HALF_OPEN and send business logic errors
		// First request transitions to HALF_OPEN
		breaker.Execute(func() (any, error) {
			return nil, consts.ErrUserNotFound
		})

		// NEW BEHAVIOR: Business logic errors counted as SUCCESS
		// Circuit will transition to CLOSED after successful requests
		for i := 0; i < 9; i++ { // 9 more (total 10)
			breaker.Execute(func() (any, error) {
				return nil, consts.ErrUserNotFound
			})
		}

		state := breaker.State()

		// Circuit should transition to CLOSED because business logic errors = success
		if state != circuitbreaker.StateClosed {
			t.Errorf("Expected CLOSED state after business logic successes, got %s", state)
		} else {
			t.Logf("SUCCESS: Circuit recovered to CLOSED state (business logic errors counted as success)")
		}
	})

	t.Run("NewBehavior_WithSystemFailures", func(t *testing.T) {
		// Test that NEW behavior correctly handles actual system failures in half-open
		// Circuit should transition to OPEN when half-open requests are exhausted

		settings := circuitbreaker.DefaultSettings("test_system_halfopen")
		settings.MaxRequests = 10
		settings.Interval = 0
		settings.Timeout = 100 * time.Millisecond
		settings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)

			// Check if half-open exhausted
			if counts.Requests >= settings.MaxRequests && failureRatio >= 0.5 {
				return true
			}

			return counts.Requests >= 5 && failureRatio >= 0.5
		}
		settings.IsSuccessful = func(err error) bool {
			return err == nil
		}

		breaker := circuitbreaker.NewCircuitBreaker(settings)

		// Force to OPEN
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) {
				return nil, errors.New("system failure")
			})
		}

		// Wait for timeout
		time.Sleep(150 * time.Millisecond)

		// Trigger HALF_OPEN with one failing request
		breaker.Execute(func() (any, error) {
			return nil, errors.New("still broken")
		})

		if breaker.State() != circuitbreaker.StateHalfOpen {
			t.Fatalf("Expected HALF_OPEN, got %s", breaker.State())
		}

		// Send 9 more failing requests (total 10)
		// After MaxRequests (10) with all failures, circuit should trip to OPEN
		for i := 0; i < 9; i++ {
			breaker.Execute(func() (any, error) {
				return nil, errors.New("still broken")
			})
			// Check if circuit has tripped to OPEN
			if breaker.State() == circuitbreaker.StateOpen {
				t.Logf("Circuit tripped to OPEN after %d requests in half-open (as expected)", i+2)
				break
			}
		}

		state := breaker.State()

		// Circuit should transition back to OPEN (all requests failed)
		if state != circuitbreaker.StateOpen {
			t.Errorf("Expected OPEN state after system failures in half-open, got %s", state)
		} else {
			t.Logf("SUCCESS: Circuit correctly transitioned to OPEN after half-open exhausted with failures")
		}
	})
}

// TestCircuitBreakerHalfOpenRecovery tests various recovery scenarios
func TestCircuitBreakerHalfOpenRecovery(t *testing.T) {
	t.Run("PartialRecovery", func(t *testing.T) {
		// Test scenario: 6 successes, 4 failures in half-open (60% success)
		// Circuit should close

		settings := circuitbreaker.DefaultSettings("test_partial")
		settings.MaxRequests = 10
		settings.Interval = 0
		settings.Timeout = 100 * time.Millisecond
		settings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			if counts.Requests >= settings.MaxRequests && failureRatio >= 0.5 {
				return true
			}
			return counts.Requests >= 5 && failureRatio >= 0.5
		}
		settings.IsSuccessful = func(err error) bool {
			return err == nil
		}

		breaker := circuitbreaker.NewCircuitBreaker(settings)

		// Force to OPEN
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) {
				return nil, errors.New("failure")
			})
		}

		time.Sleep(150 * time.Millisecond)

		// Mix of successes and failures in half-open
		for i := 0; i < 10; i++ {
			breaker.Execute(func() (any, error) {
				if i < 6 {
					return nil, nil // Success
				}
				return nil, errors.New("failure") // Failure
			})
		}

		counts := breaker.Counts()
		state := breaker.State()

		// 4 failures out of 10 = 40% failure rate < 50% threshold
		// Circuit should NOT trip, should remain in testing mode or close
		// Since we don't have explicit half-open completion logic,
		// it should close on first success in half-open
		if state == circuitbreaker.StateOpen {
			t.Errorf("Circuit should not be OPEN with only 40%% failure rate, got state=%s (failures=%d, successes=%d)",
				state, counts.TotalFailures, counts.TotalSuccesses)
		}

		t.Logf("Partial recovery: state=%s, failures=%d, successes=%d", state, counts.TotalFailures, counts.TotalSuccesses)
	})
}
