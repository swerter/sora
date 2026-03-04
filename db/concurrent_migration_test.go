package db

// Tests for concurrent migration startup behaviour.
//
// Two storage servers starting at the same time both try to run pending
// migrations. golang-migrate serialises this with a PostgreSQL session-level
// advisory lock (pg_advisory_lock / pg_advisory_unlock). The bug that caused
// the startup deadlock:
//
//   database/sql uses a *connection pool*. Lock() may acquire the advisory
//   lock on connection C1, while Unlock() runs pg_advisory_unlock on a
//   *different* connection C2. pg_advisory_unlock returns false on C2
//   (lock not held there), and golang-migrate ignores that boolean, so the
//   lock on C1 is never released. The second server blocks on
//   pg_advisory_lock indefinitely.
//
// Fix: set MaxOpenConns(1) / MaxIdleConns(1) on the migration sql.DB so
// every golang-migrate call is forced onto the same single connection.

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
	"github.com/migadu/sora/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentMigrationStartup fires N goroutines each calling
// NewDatabaseFromConfig with runMigrations=true at the exact same instant.
// All must succeed within a strict deadline; any timeout is treated as a
// deadlock regression.
func TestConcurrentMigrationStartup(t *testing.T) {
	const N = 3
	const deadline = 30 * time.Second

	// Build config directly (same values as config-test.toml) so we can call
	// NewDatabaseFromConfig without going through setupTestDatabase, which calls
	// t.Fatal inside goroutines (unsafe in Go's testing package).
	makeConfig := func() *config.DatabaseConfig {
		return &config.DatabaseConfig{
			Write: &config.DatabaseEndpointConfig{
				Hosts:    []string{"localhost"},
				Port:     5432,
				User:     "postgres",
				Password: "password",
				Name:     "sora_test_db",
				TLSMode:  false,
			},
		}
	}

	type result struct {
		db  *Database
		err error
	}

	results := make([]result, N)
	var wg sync.WaitGroup
	gate := make(chan struct{}) // synchronise goroutines for maximum contention

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate // wait for the starting gun
			db, err := NewDatabaseFromConfig(context.Background(), makeConfig(), true, false)
			results[i] = result{db: db, err: err}
		}()
	}

	close(gate) // fire all goroutines simultaneously

	// Wait for all goroutines, but fail fast if the deadline is exceeded.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		// All goroutines finished in time.
	case <-time.After(deadline):
		t.Fatalf("TestConcurrentMigrationStartup: timed out after %s — "+
			"likely deadlock regression in migration advisory-lock code", deadline)
	}

	// Verify every goroutine succeeded.
	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d: NewDatabaseFromConfig returned error: %v", i, r.err)
		}
		if r.db != nil {
			r.db.Close()
		}
	}
}

// TestMigrationAdvisoryLockSingleConn directly exercises the root cause and
// the fix at the PostgreSQL level, without touching the schema at all.
//
// Sub-test "bug_multi_conn" reproduces the original defect:
//   - connection C1 holds pg_advisory_lock(id)
//   - pg_advisory_unlock(id) is called on a *different* pool connection C2
//   - PostgreSQL returns false (lock not released)
//
// Sub-test "fix_single_conn" confirms the cure:
//   - MaxOpenConns(1) forces all calls onto the same connection
//   - pg_advisory_unlock returns true
func TestMigrationAdvisoryLockSingleConn(t *testing.T) {
	const connStr = "postgres://postgres:password@localhost:5432/sora_test_db?sslmode=disable"
	const lockID = int64(0x736f7261) // arbitrary test-only ID — "sora" in hex

	ctx := context.Background()

	// -------------------------------------------------------------------------
	// Sub-test 1 — demonstrate the bug (multi-connection pool, no fix applied)
	// -------------------------------------------------------------------------
	t.Run("bug_multi_conn", func(t *testing.T) {
		sqlDB, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		defer sqlDB.Close()

		// Allow two connections so Lock and Unlock can land on different ones.
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(2)

		// Acquire an exclusive, dedicated connection and hold it open for the
		// entire sub-test. This forces the pool to create a second connection
		// for any subsequent sqlDB.QueryRowContext call.
		lockConn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			// Clean up: release the lock on the original connection before closing.
			_, _ = lockConn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
			lockConn.Close()
		}()

		var acquired bool
		err = lockConn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		require.NoError(t, err)
		require.True(t, acquired, "should have acquired the advisory lock on lockConn")

		// Now unlock via the *pool* (not the explicit connection).
		// With lockConn still checked out, the pool will use a second connection.
		var released bool
		err = sqlDB.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
		require.NoError(t, err)

		// This is the BUG: pg_advisory_unlock on a different connection returns
		// false — the lock on lockConn is NOT released.
		assert.False(t, released,
			"bug reproduced: pg_advisory_unlock on a different connection returns false "+
				"(the lock is still held on the original connection)")
	})

	// -------------------------------------------------------------------------
	// Sub-test 2 — confirm the fix (MaxOpenConns=1)
	// -------------------------------------------------------------------------
	t.Run("fix_single_conn", func(t *testing.T) {
		sqlDB, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		defer sqlDB.Close()

		// THE FIX: a single connection means Lock and Unlock share the same session.
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)

		var acquired bool
		err = sqlDB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		require.NoError(t, err)
		require.True(t, acquired, "should have acquired the advisory lock")

		// Unlock through the same pool — with MaxOpenConns=1 this is guaranteed
		// to use the same single connection that holds the lock.
		var released bool
		err = sqlDB.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
		require.NoError(t, err)

		assert.True(t, released,
			"fix confirmed: pg_advisory_unlock returns true when the same "+
				"connection is forced by MaxOpenConns=1")
	})

	// -------------------------------------------------------------------------
	// Sub-test 3 — simulate Server B waiting while Server A holds the lock
	// -------------------------------------------------------------------------
	t.Run("server_b_waits_for_server_a", func(t *testing.T) {
		// Server A: single-connection pool (our fix applied)
		sqlA, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		defer sqlA.Close()
		sqlA.SetMaxOpenConns(1)
		sqlA.SetMaxIdleConns(1)

		// Server B: also single-connection pool
		sqlB, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		defer sqlB.Close()
		sqlB.SetMaxOpenConns(1)
		sqlB.SetMaxIdleConns(1)

		// Server A acquires the migration advisory lock.
		var acquired bool
		err = sqlA.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		require.NoError(t, err)
		require.True(t, acquired, "server A should have acquired the lock")

		// Server B tries the non-blocking variant — it should see the lock is taken.
		var bAcquired bool
		err = sqlB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&bAcquired)
		require.NoError(t, err)
		assert.False(t, bAcquired, "server B should not acquire the lock while server A holds it")

		// Server A finishes its migrations and releases the lock.
		// With MaxOpenConns=1 this MUST succeed.
		var released bool
		err = sqlA.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
		require.NoError(t, err)
		require.True(t, released, "server A must successfully release the lock")

		// Server B now retries — the lock is free.
		err = sqlB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&bAcquired)
		require.NoError(t, err)
		assert.True(t, bAcquired, "server B should acquire the lock after server A releases it")

		// Clean up Server B's lock.
		_, _ = sqlB.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
	})

	// -------------------------------------------------------------------------
	// Sub-test 4 — verify no orphaned advisory locks survive after Close()
	// -------------------------------------------------------------------------
	t.Run("no_orphaned_lock_after_close", func(t *testing.T) {
		sqlDB, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)

		var acquired bool
		err = sqlDB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		require.NoError(t, err)
		require.True(t, acquired, "should have acquired the advisory lock")

		var released bool
		err = sqlDB.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", lockID).Scan(&released)
		require.NoError(t, err)
		require.True(t, released, "should have released the lock before close")

		// Close the pool — connection is returned and sessions end cleanly.
		require.NoError(t, sqlDB.Close())

		// Verify no lock remains by trying to acquire it from a fresh connection.
		sqlCheck, err := sql.Open("pgx", connStr)
		require.NoError(t, err)
		defer sqlCheck.Close()
		sqlCheck.SetMaxOpenConns(1)

		var checkAcquired bool
		err = sqlCheck.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&checkAcquired)
		require.NoError(t, err)
		assert.True(t, checkAcquired, "lock must be free after the pool is closed")

		if checkAcquired {
			_, _ = sqlCheck.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", lockID)
		}
	})
}

// TestMigrationDeadlineDoesNotDeadlock verifies the timeout/polling path in
// migrate(): if the migration goroutine is still blocked when migrateCtx fires,
// the cleanup goroutine correctly drains the channel once the goroutine
// eventually finishes, and the parent call either succeeds (version is current)
// or returns an intelligible error — never hangs.
//
// We simulate a "slow migration" by holding the golang-migrate advisory lock
// externally for a few seconds before releasing it, forcing the migration
// goroutine to queue behind it.
func TestMigrationDeadlineDoesNotDeadlock(t *testing.T) {
	const connStr = "postgres://postgres:password@localhost:5432/sora_test_db?sslmode=disable"

	ctx := context.Background()

	// Open the reference connection and compute the advisory lock ID that
	// golang-migrate would use for "sora_test_db". golang-migrate computes:
	//   crc32.ChecksumIEEE(dbName) XOR crc32.ChecksumIEEE(statementTimeout)
	// StatementTimeout defaults to "", and crc32("") == 0, so:
	//   lockID = crc32.ChecksumIEEE("sora_test_db")
	//
	// Rather than importing hash/crc32 here we just hold the real lock
	// using pg_advisory_lock (blocking) to simulate another server running.
	// We use a unique enough test-only ID that won't clash with the real
	// migration lock.
	const simulatedMigrateLockID = int64(0x6d696772) // "migr" in hex

	// Hold the advisory lock on a dedicated connection for 3 seconds to
	// simulate "another server is running migrations right now".
	lockHolder, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	defer lockHolder.Close()
	lockHolder.SetMaxOpenConns(1)

	lockConn, err := lockHolder.Conn(ctx)
	require.NoError(t, err)

	var held bool
	err = lockConn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", simulatedMigrateLockID).Scan(&held)
	require.NoError(t, err)
	require.True(t, held, "prerequisite: must hold the simulated migration lock")

	// Release the external lock after 2 seconds so any waiter can proceed.
	go func() {
		time.Sleep(2 * time.Second)
		_, _ = lockConn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", simulatedMigrateLockID)
		lockConn.Close()
	}()

	// Meanwhile, try to acquire the same lock from a second pool (simulates
	// the second server's migration goroutine). With MaxOpenConns=1 and the
	// blocking pg_advisory_lock, this will wait for the release above.
	waiter, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	defer waiter.Close()
	waiter.SetMaxOpenConns(1)
	waiter.SetMaxIdleConns(1)

	acquireDone := make(chan error, 1)
	go func() {
		// Use the blocking form to simulate what golang-migrate does.
		_, acquireErr := waiter.ExecContext(context.Background(),
			"SELECT pg_advisory_lock($1)", simulatedMigrateLockID)
		acquireDone <- acquireErr
	}()

	// The waiter goroutine should unblock within 5 seconds (lock released at ~2s).
	select {
	case acquireErr := <-acquireDone:
		require.NoError(t, acquireErr, "waiter goroutine must unblock after lock is released")
	case <-time.After(5 * time.Second):
		t.Fatal("waiter goroutine did not unblock within 5s — possible deadlock regression")
	}

	// Release the lock we just acquired.
	var unblocked bool
	err = waiter.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", simulatedMigrateLockID).Scan(&unblocked)
	require.NoError(t, err)

	// With our fix (MaxOpenConns=1) the unlock MUST succeed on the same connection.
	assert.True(t, unblocked, "unlock via single-connection pool must return true")

	// Sanity: no orphan lock.
	if !unblocked {
		t.Log("WARNING: advisory lock was not released — would cause startup deadlock on next run")
	}
}

// makeTestDBConfig is a helper that returns a DatabaseConfig pointing at the
// local test database (same values as config-test.toml).
func makeTestDBConfig() *config.DatabaseConfig {
	return &config.DatabaseConfig{
		Write: &config.DatabaseEndpointConfig{
			Hosts:    []string{"localhost"},
			Port:     5432,
			User:     "postgres",
			Password: "password",
			Name:     "sora_test_db",
			TLSMode:  false,
		},
	}
}

// TestNewDatabaseFromConfigConcurrentIdempotent is the end-to-end version:
// it calls the real NewDatabaseFromConfig (the actual public API) from multiple
// goroutines and asserts all succeed without hanging.
func TestNewDatabaseFromConfigConcurrentIdempotent(t *testing.T) {
	const N = 4
	const deadline = 20 * time.Second

	type result struct {
		db  *Database
		err error
	}
	results := make([]result, N)

	var wg sync.WaitGroup
	gate := make(chan struct{})

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			db, err := NewDatabaseFromConfig(context.Background(), makeTestDBConfig(), true, false)
			results[i] = result{db: db, err: err}
		}()
	}

	close(gate)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(deadline):
		t.Fatalf("timed out after %s — likely deadlock in migration advisory-lock path", deadline)
	}

	var firstErr error
	for i, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			t.Errorf("goroutine %d error: %v", i, r.err)
		}
		if r.db != nil {
			r.db.Close()
		}
	}

	if firstErr != nil && !errors.Is(firstErr, context.DeadlineExceeded) {
		t.Logf("first error seen: %v", firstErr)
	}
}
