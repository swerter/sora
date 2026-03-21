package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupCleanerTestDatabase creates a test database and returns it along with test data
func setupCleanerTestDatabase(t *testing.T) (*Database, string, int64, int64) {
	db := setupTestDatabase(t)

	// Use test name and timestamp to create unique email
	testEmail := fmt.Sprintf("test_%s_%d@example.com", t.Name(), time.Now().UnixNano())

	ctx := context.Background()

	// Create test account
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	req := CreateAccountRequest{
		Email:     testEmail,
		Password:  "password123",
		IsPrimary: true,
		HashType:  "bcrypt",
	}
	_, err = db.CreateAccount(ctx, tx, req)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Get account ID
	accountID, err := db.GetAccountIDByAddress(ctx, testEmail)
	require.NoError(t, err)

	// Create INBOX mailbox
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	err = db.CreateMailbox(ctx, tx2, accountID, "INBOX", nil)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Get mailbox ID
	mailbox, err := db.GetMailboxByName(ctx, accountID, "INBOX")
	require.NoError(t, err)
	mailboxID := mailbox.ID

	return db, testEmail, accountID, mailboxID
}

// TestPruneOldMessageBodies tests the PruneOldMessageBodies function
func TestPruneOldMessageBodies(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	// Test setup: Create message contents with different ages
	tx3, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx3.Rollback(ctx)

	// Insert test message content with unique hashes based on test name and timestamp
	testTimestamp := time.Now().UnixNano()
	oldContentHash := fmt.Sprintf("old_%s_%d", t.Name(), testTimestamp)
	recentContentHash := fmt.Sprintf("recent_%s_%d", t.Name(), testTimestamp+1)
	expungedContentHash := fmt.Sprintf("expunged_%s_%d", t.Name(), testTimestamp+2)

	testBody := "This is a test message body that should be pruned or kept based on message age"

	// Insert message contents
	_, err = tx3.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers, created_at, updated_at)
		VALUES 
		($1, $2, to_tsvector('english', $2), '', NOW(), NOW()),
		($3, $4, to_tsvector('english', $4), '', NOW(), NOW()),
		($5, $6, to_tsvector('english', $6), '', NOW(), NOW())
	`, oldContentHash, testBody,
		recentContentHash, testBody,
		expungedContentHash, testBody)
	require.NoError(t, err)

	// Insert old message (should be pruned)
	oldSentDate := time.Now().Add(-48 * time.Hour) // 2 days ago
	_, err = tx3.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, expunged_at, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
		VALUES ($1, $2, 1, $3, $4, $4, $5, 0, NULL, TRUE, 'test-domain', 'test-localpart-1', 'msgid1', 'body', '[]', 1)
	`, accountID, mailboxID, oldContentHash, oldSentDate, len(testBody))
	require.NoError(t, err)

	// Insert recent message (should NOT be pruned)
	recentSentDate := time.Now().Add(-12 * time.Hour) // 12 hours ago
	_, err = tx3.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, expunged_at, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
		VALUES ($1, $2, 2, $3, $4, $4, $5, 0, NULL, TRUE, 'test-domain', 'test-localpart-2', 'msgid2', 'body', '[]', 2)
	`, accountID, mailboxID, recentContentHash, recentSentDate, len(testBody))
	require.NoError(t, err)

	// Insert old expunged message (should be pruned since it's expunged)
	expungedSentDate := time.Now().Add(-12 * time.Hour) // 12 hours ago but expunged
	_, err = tx3.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, expunged_at, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
		VALUES ($1, $2, 3, $3, $4, $4, $5, 0, NOW(), TRUE, 'test-domain', 'test-localpart-3', 'msgid3', 'body', '[]', 3)
	`, accountID, mailboxID, expungedContentHash, expungedSentDate, len(testBody))
	require.NoError(t, err)

	err = tx3.Commit(ctx)
	require.NoError(t, err)

	// Verify initial state - all message contents should have text_body
	var oldBodyBefore, recentBodyBefore, expungedBodyBefore string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", oldContentHash).Scan(&oldBodyBefore)
	require.NoError(t, err)
	assert.Equal(t, testBody, oldBodyBefore)

	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", recentContentHash).Scan(&recentBodyBefore)
	require.NoError(t, err)
	assert.Equal(t, testBody, recentBodyBefore)

	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", expungedContentHash).Scan(&expungedBodyBefore)
	require.NoError(t, err)
	assert.Equal(t, testBody, expungedBodyBefore)

	// Prune message bodies older than 24 hours.
	// The shared test database may have leftover data from other tests that fills
	// the 100-row batch before our test rows. Run in a loop until our old message
	// gets pruned (max 50 iterations to avoid infinite loop).
	retention := 24 * time.Hour
	var totalRowsAffected int64
	for i := 0; i < 50; i++ {
		tx4, err := db.GetWritePool().Begin(ctx)
		require.NoError(t, err)

		rowsAffected, err := db.PruneOldMessageBodies(ctx, tx4, retention)
		require.NoError(t, err)

		err = tx4.Commit(ctx)
		require.NoError(t, err)

		totalRowsAffected += rowsAffected
		if rowsAffected == 0 {
			break // No more rows to prune
		}
	}
	t.Logf("PruneOldMessageBodies affected %d rows total", totalRowsAffected)

	// Force a new snapshot by starting and immediately committing a transaction
	// This ensures we see the committed changes from the prune operations
	txSync, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	err = txSync.Commit(ctx)
	require.NoError(t, err)

	// Test 2: Verify old message body was pruned
	var oldBodyAfter *string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", oldContentHash).Scan(&oldBodyAfter)
	require.NoError(t, err)
	assert.Nil(t, oldBodyAfter, "Old message body should be NULL after pruning")

	// Test 3: Verify recent message body was NOT pruned
	var recentBodyAfter string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", recentContentHash).Scan(&recentBodyAfter)
	require.NoError(t, err)
	assert.Equal(t, testBody, recentBodyAfter, "Recent message body should still be present")

	// Test 4: Verify expunged message body was pruned (since message is expunged)
	var expungedBodyAfter *string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", expungedContentHash).Scan(&expungedBodyAfter)
	require.NoError(t, err)
	assert.Nil(t, expungedBodyAfter, "Expunged message body should be NULL after pruning")

	// Test 5: Verify text_body_tsv (search vector) is still preserved for all
	var oldTsv, recentTsv, expungedTsv string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body_tsv::text FROM message_contents WHERE content_hash = $1", oldContentHash).Scan(&oldTsv)
	require.NoError(t, err)
	assert.NotEmpty(t, oldTsv, "Search vector should be preserved even after body pruning")

	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body_tsv::text FROM message_contents WHERE content_hash = $1", recentContentHash).Scan(&recentTsv)
	require.NoError(t, err)
	assert.NotEmpty(t, recentTsv, "Search vector should be preserved")

	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body_tsv::text FROM message_contents WHERE content_hash = $1", expungedContentHash).Scan(&expungedTsv)
	require.NoError(t, err)
	assert.NotEmpty(t, expungedTsv, "Search vector should be preserved even after body pruning")

	// Test 6: Test with zero retention (should prune everything that has any expunged or old messages)
	tx5, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx5.Rollback(ctx)

	// Insert another recent message content to test zero retention
	zeroRetentionHash := fmt.Sprintf("zero_%s_%d", t.Name(), testTimestamp+3)
	_, err = tx5.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers, created_at, updated_at)
		VALUES ($1, $2, to_tsvector('english', $2), '', NOW(), NOW())
	`, zeroRetentionHash, testBody)
	require.NoError(t, err)

	// Insert message with this content
	_, err = tx5.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, expunged_at, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
		VALUES ($1, $2, 4, $3, $4, $4, $5, 0, NULL, TRUE, 'test-domain', 'test-localpart-4', 'msgid4', 'body', '[]', 4)
	`, accountID, mailboxID, zeroRetentionHash, time.Now().Add(-1*time.Minute), len(testBody))
	require.NoError(t, err)

	// Prune with zero retention - should prune the recent content that was not already pruned
	zeroRetention := time.Duration(0)
	rowsAffected2, err := db.PruneOldMessageBodies(ctx, tx5, zeroRetention)
	require.NoError(t, err)

	err = tx5.Commit(ctx)
	require.NoError(t, err)

	// Log how many rows were affected - we can't assert exact numbers due to shared test DB
	t.Logf("Zero retention prune affected %d rows total", rowsAffected2)

	t.Logf("Successfully tested PruneOldMessageBodies with email: %s", testEmail)
}

// TestPruneOldMessageBodiesEmptyDatabase tests pruning on empty database
func TestPruneOldMessageBodiesEmptyDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	// Test pruning - the shared test database may have leftover data from other tests,
	// so we just verify the function doesn't error, not the exact row count.
	retention := 24 * time.Hour
	_, err = db.PruneOldMessageBodies(ctx, tx, retention)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

// TestPruneOldMessageBodiesBatching tests that pruning works correctly with batching
func TestPruneOldMessageBodiesBatching(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	// Create a small number of old message contents to test batching logic
	// We use 5 records to verify the batch loop works correctly
	const numRecords = 5
	testTimestamp := time.Now().UnixNano()
	testBody := "Test message body for batching test"
	oldSentDate := time.Now().Add(-48 * time.Hour)

	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	// Insert multiple old message contents
	for i := 0; i < numRecords; i++ {
		contentHash := fmt.Sprintf("batch_%s_%d_%d", t.Name(), testTimestamp, i)

		// Insert message content
		_, err = tx.Exec(ctx, `
			INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers, created_at, updated_at)
			VALUES ($1, $2, to_tsvector('english', $2), '', NOW(), NOW())
		`, contentHash, testBody)
		require.NoError(t, err)

		// Insert old message
		_, err = tx.Exec(ctx, `
			INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, expunged_at, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
			VALUES ($1, $2, $3, $4, $5, $5, $6, 0, NULL, TRUE, 'test-domain', $7, $8, 'body', '[]', $3)
		`, accountID, mailboxID, 1000+i, contentHash, oldSentDate, len(testBody),
			fmt.Sprintf("test-localpart-batch-%d", i),
			fmt.Sprintf("msgid-batch-%d", i))
		require.NoError(t, err)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Prune with 24 hour retention - all test messages should be pruned
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	retention := 24 * time.Hour
	rowsAffected, err := db.PruneOldMessageBodies(ctx, tx2, retention)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, rowsAffected, int64(numRecords), "Should prune at least our test records")

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Force a new snapshot by starting and immediately committing a transaction
	// This ensures we see the committed changes from the prune operations
	txSync, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	err = txSync.Commit(ctx)
	require.NoError(t, err)

	// Verify all our test messages were pruned
	for i := 0; i < numRecords; i++ {
		contentHash := fmt.Sprintf("batch_%s_%d_%d", t.Name(), testTimestamp, i)
		var body *string
		err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", contentHash).Scan(&body)
		require.NoError(t, err)
		assert.Nil(t, body, "Message body %d should be NULL after pruning", i)
	}

	t.Logf("Successfully tested batching with %d records (email: %s)", numRecords, testEmail)
}

// TestCleanupLock tests the distributed locking mechanism for cleanup operations
func TestCleanupLock(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	// Test 1: Acquire lock successfully in a transaction
	tx1, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(ctx)

	acquired, err := db.AcquireCleanupLock(ctx, tx1)
	require.NoError(t, err)
	assert.True(t, acquired, "Should successfully acquire lock")

	// Test 2: Try to acquire lock in another transaction (should fail while tx1 holds it)
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	acquired2, err := db.AcquireCleanupLock(ctx, tx2)
	require.NoError(t, err)
	assert.False(t, acquired2, "Should not acquire lock when another transaction holds it")

	// Rollback tx2
	tx2.Rollback(ctx)

	// Test 3: Commit tx1 (releases the lock), then try to acquire in new transaction
	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// Now try to acquire in a new transaction after first was committed
	tx3, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx3.Rollback(ctx)

	acquired3, err := db.AcquireCleanupLock(ctx, tx3)
	require.NoError(t, err)
	assert.True(t, acquired3, "Should acquire lock after previous transaction committed")

	// Clean up - commit to release the lock
	err = tx3.Commit(ctx)
	require.NoError(t, err)

	t.Logf("Successfully tested cleanup locking mechanism")
}

// TestExpungeOldMessages tests automatic message expunging based on age
func TestExpungeOldMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create messages with different ages
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	// Create old message (should be expunged)
	oldCreatedAt := time.Now().Add(-72 * time.Hour) // 3 days old
	oldHash := fmt.Sprintf("old_expunge_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'old message', to_tsvector('english', 'old message'), '')
	`, oldHash)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 100, $3, $4, $4, 100, 0, TRUE, 'domain', 'part', 'msgid100', 'body', '[]', 100, $5)
	`, accountID, mailboxID, oldHash, time.Now(), oldCreatedAt)
	require.NoError(t, err)

	// Create recent message (should NOT be expunged)
	recentCreatedAt := time.Now().Add(-12 * time.Hour) // 12 hours old
	recentHash := fmt.Sprintf("recent_expunge_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'recent message', to_tsvector('english', 'recent message'), '')
	`, recentHash)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 101, $3, $4, $4, 100, 0, TRUE, 'domain', 'part', 'msgid101', 'body', '[]', 101, $5)
	`, accountID, mailboxID, recentHash, time.Now(), recentCreatedAt)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Expunge messages older than 48 hours
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	expunged, err := db.ExpungeOldMessages(ctx, tx2, 48*time.Hour)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	t.Logf("Expunged %d old messages", expunged)

	// Verify old message was expunged
	var oldExpungedAt *time.Time
	err = db.GetReadPool().QueryRow(ctx, "SELECT expunged_at FROM messages WHERE content_hash = $1", oldHash).Scan(&oldExpungedAt)
	require.NoError(t, err)
	assert.NotNil(t, oldExpungedAt, "Old message should be expunged")

	// Verify recent message was NOT expunged
	var recentExpungedAt *time.Time
	err = db.GetReadPool().QueryRow(ctx, "SELECT expunged_at FROM messages WHERE content_hash = $1", recentHash).Scan(&recentExpungedAt)
	require.NoError(t, err)
	assert.Nil(t, recentExpungedAt, "Recent message should not be expunged")

	t.Logf("Successfully tested ExpungeOldMessages with email: %s", testEmail)
}

// TestCleanupFailedUploads tests cleanup of messages that failed to upload to S3
func TestCleanupFailedUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create failed upload scenarios
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	// Create old failed upload (should be cleaned up)
	oldFailedHash := fmt.Sprintf("old_failed_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'failed upload content', to_tsvector('english', 'failed upload content'), '')
	`, oldFailedHash)
	require.NoError(t, err)

	oldCreatedAt := time.Now().Add(-25 * time.Hour) // 25 hours old
	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 200, $3, $4, $4, 100, 0, FALSE, 'domain', 'part', 'msgid200', 'body', '[]', 200, $5)
	`, accountID, mailboxID, oldFailedHash, time.Now(), oldCreatedAt)
	require.NoError(t, err)

	// Add to pending uploads
	_, err = tx.Exec(ctx, `
		INSERT INTO pending_uploads (account_id, content_hash, size, instance_id)
		VALUES ($1, $2, 100, 'test-instance')
	`, accountID, oldFailedHash)
	require.NoError(t, err)

	// Create recent failed upload (should NOT be cleaned up)
	recentFailedHash := fmt.Sprintf("recent_failed_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'recent failed content', to_tsvector('english', 'recent failed content'), '')
	`, recentFailedHash)
	require.NoError(t, err)

	recentCreatedAt := time.Now().Add(-5 * time.Hour) // 5 hours old
	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 201, $3, $4, $4, 100, 0, FALSE, 'domain', 'part', 'msgid201', 'body', '[]', 201, $5)
	`, accountID, mailboxID, recentFailedHash, time.Now(), recentCreatedAt)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
		INSERT INTO pending_uploads (account_id, content_hash, size, instance_id)
		VALUES ($1, $2, 100, 'test-instance')
	`, accountID, recentFailedHash)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Clean up failed uploads older than 24 hours
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	cleaned, err := db.CleanupFailedUploads(ctx, tx2, 24*time.Hour)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	t.Logf("Cleaned up %d failed uploads", cleaned)

	// Verify old failed upload was removed
	var oldExists int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE content_hash = $1", oldFailedHash).Scan(&oldExists)
	require.NoError(t, err)
	assert.Equal(t, 0, oldExists, "Old failed upload should be removed")

	// Verify recent failed upload still exists
	var recentExists int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE content_hash = $1", recentFailedHash).Scan(&recentExists)
	require.NoError(t, err)
	assert.Equal(t, 1, recentExists, "Recent failed upload should still exist")

	t.Logf("Successfully tested CleanupFailedUploads with email: %s", testEmail)
}

// TestGetUserScopedObjectsForCleanup tests identifying objects for cleanup
func TestGetUserScopedObjectsForCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create expunged messages ready for cleanup
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	cleanupHash := fmt.Sprintf("cleanup_ready_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'cleanup ready content', to_tsvector('english', 'cleanup ready content'), '')
	`, cleanupHash)
	require.NoError(t, err)

	expungedAt := time.Now().Add(-25 * time.Hour) // 25 hours ago
	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
		VALUES ($1, $2, 300, $3, $4, $4, 100, 0, TRUE, 'test-domain', 'test-localpart', 'msgid300', 'body', '[]', 300, $5)
	`, accountID, mailboxID, cleanupHash, time.Now(), expungedAt)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Get objects for cleanup (older than 24 hours)
	candidates, err := db.GetUserScopedObjectsForCleanup(ctx, 24*time.Hour, 10)
	require.NoError(t, err)

	t.Logf("Found %d candidates for cleanup", len(candidates))

	// Find our test candidate
	var found bool
	for _, candidate := range candidates {
		if candidate.ContentHash == cleanupHash && candidate.AccountID == accountID {
			found = true
			assert.Equal(t, "test-domain", candidate.S3Domain)
			assert.Equal(t, "test-localpart", candidate.S3Localpart)
			break
		}
	}
	assert.True(t, found, "Should find our test candidate in cleanup list")

	t.Logf("Successfully tested GetUserScopedObjectsForCleanup with email: %s", testEmail)
}

// TestDeleteExpungedMessagesByS3KeyPartsBatch tests batch deletion of expunged messages
func TestDeleteExpungedMessagesByS3KeyPartsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create expunged messages for batch deletion
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	var candidates []UserScopedObjectForCleanup
	for i := 0; i < 3; i++ {
		hash := fmt.Sprintf("batch_delete_%d_%d", testTimestamp, i)
		_, err = tx.Exec(ctx, `
			INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
			VALUES ($1, $2, to_tsvector('english', $2), '')
		`, hash, fmt.Sprintf("batch delete content %d", i))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
			VALUES ($1, $2, $3, $4, $5, $5, 100, 0, TRUE, 'batch-domain', 'batch-part', $6, 'body', '[]', $7, NOW())
		`, accountID, mailboxID, 400+i, hash, time.Now(), fmt.Sprintf("batchmsg%d", i), 400+i)
		require.NoError(t, err)

		candidates = append(candidates, UserScopedObjectForCleanup{
			AccountID:   accountID,
			ContentHash: hash,
			S3Domain:    "batch-domain",
			S3Localpart: "batch-part",
		})
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Batch delete expunged messages
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	deleted, err := db.DeleteExpungedMessagesByS3KeyPartsBatch(ctx, tx2, candidates)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted, "Should delete all 3 messages")

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Verify messages were deleted
	for i, candidate := range candidates {
		var count int
		err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE content_hash = $1", candidate.ContentHash).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, fmt.Sprintf("Message %d should be deleted", i))
	}

	t.Logf("Successfully tested DeleteExpungedMessagesByS3KeyPartsBatch with email: %s", testEmail)
}

// TestGetUnusedContentHashes tests finding orphaned content hashes
func TestGetUnusedContentHashes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create orphaned content
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	orphanedHash := fmt.Sprintf("orphaned_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'orphaned content', to_tsvector('english', 'orphaned content'), '')
	`, orphanedHash)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Get unused content hashes (use larger limit to handle test database with leftover data)
	unused, err := db.GetUnusedContentHashes(ctx, 1000)
	require.NoError(t, err)

	t.Logf("Found %d unused content hashes", len(unused))

	// Verify our orphaned hash is in the list
	var found bool
	for _, hash := range unused {
		if hash == orphanedHash {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find our orphaned hash in unused list")

	t.Logf("Successfully tested GetUnusedContentHashes")
}

// TestDeleteMessageContentsByHashBatch tests batch deletion of message contents
func TestDeleteMessageContentsByHashBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create content hashes to delete
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	var hashesToDelete []string
	for i := 0; i < 3; i++ {
		hash := fmt.Sprintf("delete_content_%d_%d", testTimestamp, i)
		_, err = tx.Exec(ctx, `
			INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
			VALUES ($1, $2, to_tsvector('english', $2), '')
		`, hash, fmt.Sprintf("content to delete %d", i))
		require.NoError(t, err)
		hashesToDelete = append(hashesToDelete, hash)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Batch delete message contents
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	deleted, err := db.DeleteMessageContentsByHashBatch(ctx, tx2, hashesToDelete)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted, "Should delete all 3 content entries")

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Verify contents were deleted
	for i, hash := range hashesToDelete {
		var count int
		err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM message_contents WHERE content_hash = $1", hash).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, fmt.Sprintf("Content %d should be deleted", i))
	}

	t.Logf("Successfully tested DeleteMessageContentsByHashBatch")
}

// TestDeleteMessageByHashAndMailbox tests targeted message deletion for re-import scenarios
func TestDeleteMessageByHashAndMailbox(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, mailboxID := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Setup: Create a message for deletion
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	deleteHash := fmt.Sprintf("delete_msg_%d", testTimestamp)
	_, err = tx.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'message to delete', to_tsvector('english', 'message to delete'), '')
	`, deleteHash)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq)
		VALUES ($1, $2, 500, $3, $4, $4, 100, 0, TRUE, 'delete-domain', 'delete-part', 'delete-msg-id', 'body', '[]', 500)
	`, accountID, mailboxID, deleteHash, time.Now())
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Test: Delete specific message by hash and mailbox
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	deleted, err := db.DeleteMessageByHashAndMailbox(ctx, tx2, accountID, mailboxID, deleteHash)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "Should delete exactly 1 message")

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Verify message was deleted
	var count int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE content_hash = $1 AND account_id = $2 AND mailbox_id = $3", deleteHash, accountID, mailboxID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Message should be deleted")

	t.Logf("Successfully tested DeleteMessageByHashAndMailbox with email: %s", testEmail)
}

// TestGetUserScopedObjectsForCleanup_LiveMessagePreventsCleanup tests the critical safety
// guarantee: S3 objects should NEVER be marked for cleanup if ANY live (non-expunged)
// message references the same content_hash, even when other messages with that hash are expunged.
// This ensures we never delete S3 objects that are still in use after operations like IMAP MOVE.
func TestGetUserScopedObjectsForCleanup_LiveMessagePreventsCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, _ := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Create two mailboxes: INBOX and Archive
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = db.CreateMailbox(ctx, tx, accountID, "Archive", nil)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Get both mailbox IDs
	inboxMailbox, err := db.GetMailboxByName(ctx, accountID, "INBOX")
	require.NoError(t, err)
	inboxID := inboxMailbox.ID

	archiveMailbox, err := db.GetMailboxByName(ctx, accountID, "Archive")
	require.NoError(t, err)
	archiveID := archiveMailbox.ID

	// Setup test scenario: Simulate IMAP MOVE operation
	// 1. Message originally in INBOX (now expunged, past grace period)
	// 2. Same message moved to Archive (live, still exists)
	// Both share the same content_hash (same S3 object)

	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	sharedContentHash := fmt.Sprintf("shared_content_%d", testTimestamp)

	// Create message content (shared by both messages)
	_, err = tx2.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'shared message body', to_tsvector('english', 'shared message body'), '')
	`, sharedContentHash)
	require.NoError(t, err)

	// Create EXPUNGED message in INBOX (old, past grace period)
	expungedAt := time.Now().Add(-25 * time.Hour) // 25 hours ago (past 24h grace)
	_, err = tx2.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
		VALUES ($1, $2, 1, $3, $4, $4, 100, 0, TRUE, 'test-domain', 'test-localpart', 'msgid-inbox', 'body', '[]', 1, $5)
	`, accountID, inboxID, sharedContentHash, time.Now().Add(-26*time.Hour), expungedAt)
	require.NoError(t, err)

	// Create LIVE message in Archive (same content_hash, but NOT expunged)
	_, err = tx2.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
		VALUES ($1, $2, 1, $3, $4, $4, 100, 0, TRUE, 'test-domain', 'test-localpart', 'msgid-archive', 'body', '[]', 2, NULL)
	`, accountID, archiveID, sharedContentHash, time.Now().Add(-26*time.Hour))
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// TEST: Get objects for cleanup (older than 24 hours)
	candidates, err := db.GetUserScopedObjectsForCleanup(ctx, 24*time.Hour, 100)
	require.NoError(t, err)

	// VERIFY: The shared content_hash should NOT appear in cleanup candidates
	// because a live message (in Archive) still references it
	for _, candidate := range candidates {
		if candidate.ContentHash == sharedContentHash && candidate.AccountID == accountID {
			t.Fatalf("SAFETY VIOLATION: Found content_hash %s in cleanup list, but live message exists in Archive mailbox! This would incorrectly delete S3 object still in use.", sharedContentHash)
		}
	}

	// Additional verification: confirm both messages exist in database
	var inboxCount, archiveCount int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3", accountID, inboxID, sharedContentHash).Scan(&inboxCount)
	require.NoError(t, err)
	assert.Equal(t, 1, inboxCount, "Expunged message should still exist in INBOX")

	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3", accountID, archiveID, sharedContentHash).Scan(&archiveCount)
	require.NoError(t, err)
	assert.Equal(t, 1, archiveCount, "Live message should exist in Archive")

	t.Logf("✓ SAFETY VERIFIED: S3 object with hash %s correctly NOT marked for cleanup despite expunged INBOX message (live Archive message prevents deletion)", sharedContentHash)
	t.Logf("Successfully tested live message safety with email: %s", testEmail)
}

// TestGetUserScopedObjectsForCleanup_AllExpungedAllowsCleanup is the counterpart to the
// live message test: verifies that when ALL messages with a content_hash are expunged
// (past grace period), the S3 object IS correctly marked for cleanup.
func TestGetUserScopedObjectsForCleanup_AllExpungedAllowsCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, _ := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Create Archive mailbox
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = db.CreateMailbox(ctx, tx, accountID, "Archive", nil)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Get both mailbox IDs
	inboxMailbox, err := db.GetMailboxByName(ctx, accountID, "INBOX")
	require.NoError(t, err)
	inboxID := inboxMailbox.ID

	archiveMailbox, err := db.GetMailboxByName(ctx, accountID, "Archive")
	require.NoError(t, err)
	archiveID := archiveMailbox.ID

	// Setup: Create same message in two mailboxes, BOTH expunged past grace period
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	allExpungedHash := fmt.Sprintf("all_expunged_%d", testTimestamp)

	// Create message content
	_, err = tx2.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, 'all expunged content', to_tsvector('english', 'all expunged content'), '')
	`, allExpungedHash)
	require.NoError(t, err)

	expungedAt := time.Now().Add(-25 * time.Hour) // 25 hours ago

	// Create EXPUNGED message in INBOX
	_, err = tx2.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
		VALUES ($1, $2, 2, $3, $4, $4, 100, 0, TRUE, 'all-domain', 'all-part', 'msgid-inbox-exp', 'body', '[]', 3, $5)
	`, accountID, inboxID, allExpungedHash, time.Now().Add(-26*time.Hour), expungedAt)
	require.NoError(t, err)

	// Create EXPUNGED message in Archive (same content_hash, also expunged)
	_, err = tx2.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, expunged_at)
		VALUES ($1, $2, 2, $3, $4, $4, 100, 0, TRUE, 'all-domain', 'all-part', 'msgid-archive-exp', 'body', '[]', 4, $5)
	`, accountID, archiveID, allExpungedHash, time.Now().Add(-26*time.Hour), expungedAt)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// TEST: Get objects for cleanup
	candidates, err := db.GetUserScopedObjectsForCleanup(ctx, 24*time.Hour, 100)
	require.NoError(t, err)

	// VERIFY: The content_hash SHOULD appear in cleanup candidates
	// because ALL messages are expunged past grace period
	var found bool
	for _, candidate := range candidates {
		if candidate.ContentHash == allExpungedHash && candidate.AccountID == accountID {
			found = true
			assert.Equal(t, "all-domain", candidate.S3Domain)
			assert.Equal(t, "all-part", candidate.S3Localpart)
			break
		}
	}
	assert.True(t, found, "Content hash should be marked for cleanup when ALL messages are expunged")

	t.Logf("✓ VERIFIED: S3 object with hash %s correctly marked for cleanup (all messages expunged past grace period)", allExpungedHash)
	t.Logf("Successfully tested all-expunged cleanup with email: %s", testEmail)
}

// TestCleanerWorkflow_MovedMessageS3Preservation tests the complete cleaner workflow:
// 1. Message is moved from INBOX to Archive (IMAP MOVE)
// 2. Time passes beyond grace period
// 3. Cleaner runs and removes expunged database row
// 4. S3 object is preserved because live message still references it
// 5. Live message in Archive continues to work correctly
//
// This is the most realistic end-to-end test of the safety guarantee.
func TestCleanerWorkflow_MovedMessageS3Preservation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, testEmail, accountID, _ := setupCleanerTestDatabase(t)
	defer db.Close()

	ctx := context.Background()
	testTimestamp := time.Now().UnixNano()

	// Step 1: Create Archive mailbox
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = db.CreateMailbox(ctx, tx, accountID, "Archive", nil)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	inboxMailbox, err := db.GetMailboxByName(ctx, accountID, "INBOX")
	require.NoError(t, err)
	inboxID := inboxMailbox.ID

	archiveMailbox, err := db.GetMailboxByName(ctx, accountID, "Archive")
	require.NoError(t, err)
	archiveID := archiveMailbox.ID

	// Step 2: Create original message in INBOX
	tx2, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(ctx)

	contentHash := fmt.Sprintf("moved_msg_%d", testTimestamp)
	messageBody := "This is the message body that will be moved"

	// Insert message content
	_, err = tx2.Exec(ctx, `
		INSERT INTO message_contents (content_hash, text_body, text_body_tsv, headers)
		VALUES ($1, $2, to_tsvector('english', $2), 'Subject: Test Message')
	`, contentHash, messageBody)
	require.NoError(t, err)

	// Insert message in INBOX (will be moved later)
	oldCreatedAt := time.Now().Add(-48 * time.Hour) // Created 48 hours ago
	_, err = tx2.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 1, $3, $4, $4, $5, 0, TRUE, 'move-domain', 'move-part', 'moved-msg-id', 'body', '[]', 1, $6)
	`, accountID, inboxID, contentHash, time.Now().Add(-48*time.Hour), len(messageBody), oldCreatedAt)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	// Verify message exists in INBOX
	var inboxCountBefore int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3 AND expunged_at IS NULL", accountID, inboxID, contentHash).Scan(&inboxCountBefore)
	require.NoError(t, err)
	assert.Equal(t, 1, inboxCountBefore, "Message should exist in INBOX before MOVE")

	// Step 3: Simulate IMAP MOVE - mark original as expunged and create copy in Archive
	tx3, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx3.Rollback(ctx)

	// Mark INBOX message as expunged (25 hours ago - past grace period)
	expungedAt := time.Now().Add(-25 * time.Hour)
	_, err = tx3.Exec(ctx, `
		UPDATE messages
		SET expunged_at = $1
		WHERE account_id = $2 AND mailbox_id = $3 AND content_hash = $4
	`, expungedAt, accountID, inboxID, contentHash)
	require.NoError(t, err)

	// Create new message in Archive (same content_hash - shared S3 object)
	_, err = tx3.Exec(ctx, `
		INSERT INTO messages (account_id, mailbox_id, uid, content_hash, sent_date, internal_date, size, flags, uploaded, s3_domain, s3_localpart, message_id, body_structure, recipients_json, created_modseq, created_at)
		VALUES ($1, $2, 1, $3, $4, $4, $5, 0, TRUE, 'move-domain', 'move-part', 'moved-msg-id', 'body', '[]', 2, NOW())
	`, accountID, archiveID, contentHash, time.Now().Add(-48*time.Hour), len(messageBody))
	require.NoError(t, err)

	err = tx3.Commit(ctx)
	require.NoError(t, err)

	// Verify state after MOVE
	var inboxExpungedCount, archiveLiveCount int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3 AND expunged_at IS NOT NULL", accountID, inboxID, contentHash).Scan(&inboxExpungedCount)
	require.NoError(t, err)
	assert.Equal(t, 1, inboxExpungedCount, "INBOX message should be expunged")

	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3 AND expunged_at IS NULL", accountID, archiveID, contentHash).Scan(&archiveLiveCount)
	require.NoError(t, err)
	assert.Equal(t, 1, archiveLiveCount, "Archive message should be live")

	// Step 4: Run GetUserScopedObjectsForCleanup - should NOT mark S3 for deletion
	candidates, err := db.GetUserScopedObjectsForCleanup(ctx, 24*time.Hour, 100)
	require.NoError(t, err)

	for _, candidate := range candidates {
		if candidate.ContentHash == contentHash && candidate.AccountID == accountID {
			t.Fatalf("WORKFLOW VIOLATION: S3 object marked for deletion despite live Archive message! content_hash=%s", contentHash)
		}
	}
	t.Logf("✓ Step 4 PASSED: S3 object correctly NOT marked for deletion")

	// Step 5: Run DeleteExpungedMessagesByS3KeyPartsBatch to clean up database rows
	// Even though S3 is not marked for cleanup, we should still be able to delete
	// the expunged database row (this is safe because another row references the S3 object)
	tx4, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx4.Rollback(ctx)

	// Manually create a candidate for the expunged INBOX message (simulating what would happen
	// if we incorrectly marked it for cleanup - but we're just testing DB deletion here)
	fakeCandidate := []UserScopedObjectForCleanup{
		{
			AccountID:   accountID,
			ContentHash: contentHash,
			S3Domain:    "move-domain",
			S3Localpart: "move-part",
		},
	}

	deleted, err := db.DeleteExpungedMessagesByS3KeyPartsBatch(ctx, tx4, fakeCandidate)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "Should delete the expunged INBOX message row")

	err = tx4.Commit(ctx)
	require.NoError(t, err)

	t.Logf("✓ Step 5 PASSED: Expunged database row deleted (%d rows)", deleted)

	// Step 6: Verify final state
	// - INBOX message should be gone from database
	var inboxFinalCount int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3", accountID, inboxID, contentHash).Scan(&inboxFinalCount)
	require.NoError(t, err)
	assert.Equal(t, 0, inboxFinalCount, "INBOX message should be deleted from database")

	// - Archive message should still exist
	var archiveFinalCount int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE account_id = $1 AND mailbox_id = $2 AND content_hash = $3", accountID, archiveID, contentHash).Scan(&archiveFinalCount)
	require.NoError(t, err)
	assert.Equal(t, 1, archiveFinalCount, "Archive message should still exist")

	// - Message content should still exist (not cleaned up)
	var contentExists int
	err = db.GetReadPool().QueryRow(ctx, "SELECT COUNT(*) FROM message_contents WHERE content_hash = $1", contentHash).Scan(&contentExists)
	require.NoError(t, err)
	assert.Equal(t, 1, contentExists, "Message content should still exist")

	// - Verify we can still read the message body (S3 object would still be accessible)
	var bodyText *string
	err = db.GetReadPool().QueryRow(ctx, "SELECT text_body FROM message_contents WHERE content_hash = $1", contentHash).Scan(&bodyText)
	require.NoError(t, err)
	require.NotNil(t, bodyText, "Message body should be accessible")
	assert.Equal(t, messageBody, *bodyText, "Message body content should match")

	t.Logf("✓ Step 6 PASSED: Final state verified")
	t.Logf("  - INBOX expunged row: DELETED ✓")
	t.Logf("  - Archive live message: EXISTS ✓")
	t.Logf("  - S3 object (content_hash): PRESERVED ✓")
	t.Logf("  - Message body accessible: YES ✓")
	t.Logf("")
	t.Logf("✅ COMPLETE WORKFLOW VERIFIED: Moved message cleanup is safe!")
	t.Logf("Successfully tested with email: %s", testEmail)
}
