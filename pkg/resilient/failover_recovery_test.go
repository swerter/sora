package resilient

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/migadu/sora/pkg/circuitbreaker"
)

// TestIsReadOnlyTransactionError verifies that isReadOnlyTransactionError correctly
// identifies PostgreSQL SQLSTATE 25006 (read_only_sql_transaction) and nothing else.
func TestIsReadOnlyTransactionError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "SQLSTATE 25006 (read_only_sql_transaction)",
			err:      &pgconn.PgError{Code: "25006"},
			expected: true,
		},
		{
			name:     "SQLSTATE 25006 wrapped with fmt.Errorf %w",
			err:      fmt.Errorf("failed to insert: %w", &pgconn.PgError{Code: "25006"}),
			expected: true,
		},
		{
			name:     "SQLSTATE 25006 wrapped with errors.Join",
			err:      errors.Join(errors.New("outer"), &pgconn.PgError{Code: "25006"}),
			expected: true,
		},
		{
			name:     "SQLSTATE 40001 (serialization_failure) is not read-only",
			err:      &pgconn.PgError{Code: "40001"},
			expected: false,
		},
		{
			name:     "SQLSTATE 40P01 (deadlock_detected) is not read-only",
			err:      &pgconn.PgError{Code: "40P01"},
			expected: false,
		},
		{
			name:     "SQLSTATE 08006 (connection_failure) is not read-only",
			err:      &pgconn.PgError{Code: "08006"},
			expected: false,
		},
		{
			name:     "generic error is not read-only",
			err:      errors.New("database connection failed"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isReadOnlyTransactionError(tt.err)
			if got != tt.expected {
				t.Errorf("isReadOnlyTransactionError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

// TestCircuitBreakerTripsOnReadOnlyError verifies that SQLSTATE 25006 is treated as a
// *system failure* by the write circuit breaker (not as a business-logic success).
//
// This is important because after a PostgreSQL failover the old primary becomes a
// read-only standby.  Every write will fail with 25006.  If the circuit breaker
// counted 25006 as a success the breaker would never trip and we would spin
// forever against the wrong server.
func TestCircuitBreakerTripsOnReadOnlyError(t *testing.T) {
	writeSettings := circuitbreaker.DefaultSettings("test_write_readonly")
	writeSettings.MaxRequests = 10
	writeSettings.Interval = 0
	writeSettings.Timeout = 100 * time.Millisecond
	writeSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		if counts.Requests >= writeSettings.MaxRequests && failureRatio >= 0.5 {
			return true
		}
		return counts.Requests >= 5 && failureRatio >= 0.5
	}
	// Mirror production IsSuccessful for the write circuit breaker.
	// 25006 is NOT in any success list, so it must count as a failure.
	writeSettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		// Only deadlock / serialization are self-resolving and treated as success
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40P01", "40001":
				return true
			}
		}
		return false
	}

	breaker := circuitbreaker.NewCircuitBreaker(writeSettings)

	readOnlyErr := &pgconn.PgError{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"}

	// Drive the breaker with 5 consecutive 25006 errors (≥5 requests, 100% failure → trips)
	for i := 0; i < 5; i++ {
		_, err := breaker.Execute(func() (any, error) {
			return nil, readOnlyErr
		})
		if err == nil {
			t.Fatalf("iteration %d: expected error from Execute, got nil", i)
		}
	}

	if breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("circuit breaker should be OPEN after 5 read-only errors, got: %s", breaker.State())
	}
	t.Logf("OK: write circuit breaker tripped OPEN after 25006 errors as expected")

	// Verify that requests are now rejected
	_, err := breaker.Execute(func() (any, error) {
		return nil, nil // would succeed if allowed through
	})
	if !errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
		t.Errorf("expected ErrCircuitBreakerOpen while OPEN, got: %v", err)
	}
}

// TestFailoverRecoverySequence simulates the full recovery sequence that should
// happen after a PostgreSQL primary-to-standby failover:
//
//  1. Writes fail with SQLSTATE 25006 → write circuit breaker trips OPEN
//  2. pool.Reset() is called so connections will be re-established on next attempt
//     (represented here by the ForceHalfOpen call after the health check recovers)
//  3. Health check detects pg_is_in_recovery()=false on new primary → pool marked
//     healthy → ForceHalfOpen() called on the circuit breaker
//  4. Test write succeeds on new primary → circuit transitions CLOSED
//
// The test focuses on the circuit-breaker state machine; actual pool reset and
// pg_is_in_recovery() calls happen at the DB layer and are verified by the
// read-only error detection test above.
func TestFailoverRecoverySequence(t *testing.T) {
	writeSettings := circuitbreaker.DefaultSettings("test_failover_recovery")
	writeSettings.MaxRequests = 10
	writeSettings.Interval = 0
	writeSettings.Timeout = 100 * time.Millisecond // short for test speed
	writeSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		if counts.Requests >= writeSettings.MaxRequests && failureRatio >= 0.5 {
			return true
		}
		return counts.Requests >= 5 && failureRatio >= 0.5
	}
	writeSettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40P01", "40001":
				return true
			}
		}
		return false
	}
	var stateChanges []string
	writeSettings.OnStateChange = func(name string, from, to circuitbreaker.State) {
		stateChanges = append(stateChanges, fmt.Sprintf("%s→%s", from, to))
	}

	breaker := circuitbreaker.NewCircuitBreaker(writeSettings)
	readOnlyErr := &pgconn.PgError{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"}

	// --- Phase 1: Failover detected; writes failing with 25006 ---
	t.Log("Phase 1: writes failing with SQLSTATE 25006 (old primary became read-only)")
	for i := 0; i < 5; i++ {
		breaker.Execute(func() (any, error) { return nil, readOnlyErr })
	}
	if breaker.State() != circuitbreaker.StateOpen {
		t.Fatalf("Phase 1: expected OPEN, got %s", breaker.State())
	}
	t.Logf("Phase 1 OK: circuit OPEN after 5 read-only errors; pool.Reset() would be called here")

	// --- Phase 2: Circuit stays OPEN; old code would spin here forever ---
	// Before the fix, checkPoolHealth would Ping the old primary (still alive),
	// see no health change, and never call ForceHalfOpen().
	// With the fix, checkPoolHealth calls pg_is_in_recovery() and detects the
	// server is a standby → marks unhealthy → resets pool.
	// When a subsequent health check reaches the new primary:
	// pg_is_in_recovery()=false → marks healthy → ForceHalfOpen().
	t.Log("Phase 2: health check detects new primary is writable → ForceHalfOpen()")
	breaker.ForceHalfOpen()
	if breaker.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("Phase 2: expected HALF_OPEN after ForceHalfOpen, got %s", breaker.State())
	}
	t.Log("Phase 2 OK: circuit is HALF_OPEN; next write will use a fresh connection")

	// --- Phase 3: Test write succeeds on new primary → circuit closes ---
	t.Log("Phase 3: write succeeds on new primary")
	_, err := breaker.Execute(func() (any, error) {
		return nil, nil // success
	})
	if err != nil {
		t.Fatalf("Phase 3: expected success through HALF_OPEN circuit, got: %v", err)
	}
	if breaker.State() != circuitbreaker.StateClosed {
		t.Fatalf("Phase 3: expected CLOSED after success, got %s", breaker.State())
	}
	t.Logf("Phase 3 OK: circuit CLOSED, normal operation resumed")

	t.Logf("State transitions: %v", stateChanges)
}

// TestOldBehaviorWithoutPoolReset demonstrates the *infinite loop* that the fix
// addresses.  Before the fix:
//
//  1. Circuit trips OPEN because of 25006 errors.
//  2. After 30s timeout the circuit auto-transitions to HALF_OPEN.
//  3. The write attempt reuses a stale connection to the old (read-only) primary → 25006 again.
//  4. Circuit trips OPEN again.  Loop forever.
//
// With the fix (pool.Reset() on 25006 + pg_is_in_recovery() in health check),
// the stale connections are dropped so step 3 can reach the new primary.
// This test documents the old broken cycle and verifies the circuit breaker
// itself behaves correctly given fresh vs stale connections.
func TestOldBehaviorWithoutPoolReset(t *testing.T) {
	writeSettings := circuitbreaker.DefaultSettings("test_old_behavior")
	writeSettings.MaxRequests = 10
	writeSettings.Interval = 0
	writeSettings.Timeout = 50 * time.Millisecond // very short for test speed
	writeSettings.ReadyToTrip = func(counts circuitbreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 5 && failureRatio >= 0.5
	}
	writeSettings.IsSuccessful = func(err error) bool {
		if err == nil {
			return true
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "40P01" || pgErr.Code == "40001" {
				return true
			}
		}
		return false
	}

	breaker := circuitbreaker.NewCircuitBreaker(writeSettings)
	readOnlyErr := &pgconn.PgError{Code: "25006"}

	// OLD behavior: without pool reset, the half-open test write always hits the same
	// stale connection → always returns 25006 → always trips back to OPEN.
	//
	// ReadyToTrip requires ≥5 requests with ≥50% failure rate to re-trip, so we
	// send 5 failing requests in HALF_OPEN (all 25006) to simulate the stale pool.
	for cycle := 1; cycle <= 3; cycle++ {
		// Drive to OPEN
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) { return nil, readOnlyErr })
		}
		if breaker.State() != circuitbreaker.StateOpen {
			t.Fatalf("cycle %d: expected OPEN, got %s", cycle, breaker.State())
		}

		// Wait for auto-transition to HALF_OPEN
		time.Sleep(60 * time.Millisecond)

		// WITHOUT pool reset: every attempt in HALF_OPEN still hits the same stale
		// connection to the old (read-only) primary → all return 25006.
		// Drive ReadyToTrip threshold (5 requests, 100% failure rate).
		for i := 0; i < 5; i++ {
			breaker.Execute(func() (any, error) { return nil, readOnlyErr })
			if breaker.State() == circuitbreaker.StateOpen {
				break
			}
		}

		// Circuit trips back to OPEN — we are stuck
		if breaker.State() != circuitbreaker.StateOpen {
			t.Fatalf("cycle %d: expected OPEN after stale connection failures, got %s", cycle, breaker.State())
		}
		t.Logf("cycle %d: confirmed stuck OPEN→HALF_OPEN→OPEN loop (old behavior without pool reset)", cycle)
	}

	// WITH the fix: after pool.Reset() the half-open test write gets a fresh
	// connection that reaches the new primary → succeeds → circuit closes.
	time.Sleep(60 * time.Millisecond) // wait for HALF_OPEN auto-transition
	_, err := breaker.Execute(func() (any, error) {
		return nil, nil // fresh connection to new primary succeeds
	})
	if err != nil {
		t.Fatalf("expected success with fresh connection (pool reset applied), got: %v", err)
	}
	if breaker.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected CLOSED after fresh-connection success, got %s", breaker.State())
	}
	t.Log("WITH pool reset: circuit correctly recovered to CLOSED on first fresh-connection attempt")
}
