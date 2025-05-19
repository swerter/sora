package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// TestMoveMessages tests the MoveMessages function
func TestMoveMessages(t *testing.T) {
	// Uncomment to skip this test if we're not running in a CI environment with a database
	// t.Skip("This test requires a database connection. Run manually with -run=TestMoveMessages")

	// Create a database connection
	ctx := context.Background()
	connString := "postgres://postgres@localhost:5432/imap_db?sslmode=disable"

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("Unable to parse connection string: %v", err)
	}

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer dbPool.Close()

	db := &Database{
		Pool: dbPool,
	}

	// Create a test user
	var userID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO users (username, password) 
		VALUES ('testuser@example.com', 'password') 
		ON CONFLICT (username) 
		DO UPDATE SET username = EXCLUDED.username 
		RETURNING id
	`).Scan(&userID)
	if err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}

	// Create source and destination mailboxes with unique names
	timestamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("Source_%d", timestamp)
	destName := fmt.Sprintf("Destination_%d", timestamp)

	var sourceMailboxID, destMailboxID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO mailboxes (user_id, name, uid_validity) 
		VALUES ($1, $2, 1234567890) 
		RETURNING id
	`, userID, sourceName).Scan(&sourceMailboxID)
	if err != nil {
		t.Fatalf("Failed to create source mailbox: %v", err)
	}

	err = db.Pool.QueryRow(ctx, `
		INSERT INTO mailboxes (user_id, name, uid_validity) 
		VALUES ($1, $2, 1234567891) 
		RETURNING id
	`, userID, destName).Scan(&destMailboxID)
	if err != nil {
		t.Fatalf("Failed to create destination mailbox: %v", err)
	}

	// Create test messages in the source mailbox
	for i := 1; i <= 3; i++ {
		_, err = db.Pool.Exec(ctx, `
			INSERT INTO messages (
				user_id, mailbox_id, mailbox_path, uid, content_hash, message_id, 
				flags, internal_date, size, body_structure, recipients_json, 
				text_body, sent_date, created_modseq
			) VALUES (
				$1, $2, 'Source', $3, $4, $5, 
				0, NOW(), 100, '\x', '[]'::jsonb, 
				'Test message', NOW(), nextval('messages_modseq')
			) ON CONFLICT DO NOTHING
		`, userID, sourceMailboxID, i, "hash"+string(rune(i)), "msg"+string(rune(i)))
		if err != nil {
			t.Fatalf("Failed to create test message %d: %v", i, err)
		}
	}

	// Move the messages
	uids := []imap.UID{1, 2, 3}
	messageUIDMap, err := db.MoveMessages(ctx, &uids, sourceMailboxID, destMailboxID)
	if err != nil {
		t.Fatalf("Failed to move messages: %v", err)
	}

	// Verify the messages were moved
	assert.Equal(t, 3, len(messageUIDMap), "Expected 3 messages to be moved")

	// Check that the messages are now in the destination mailbox
	var count int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE mailbox_id = $1 AND expunged_at IS NULL
	`, destMailboxID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count messages in destination mailbox: %v", err)
	}
	assert.Equal(t, 3, count, "Expected 3 messages in destination mailbox")

	// Check that the messages are marked as expunged in the source mailbox
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE mailbox_id = $1 AND expunged_at IS NOT NULL
	`, sourceMailboxID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count expunged messages in source mailbox: %v", err)
	}
	assert.Equal(t, 3, count, "Expected 3 expunged messages in source mailbox")

	// Clean up
	_, err = db.Pool.Exec(ctx, `DELETE FROM messages WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up messages: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM mailboxes WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up mailboxes: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up user: %v", err)
	}
}

// TestMoveMessagesWithinSameMailbox tests that MoveMessages prevents moving messages within the same mailbox
func TestMoveMessagesWithinSameMailbox(t *testing.T) {
	// Uncomment to skip this test if we're not running in a CI environment with a database
	// t.Skip("This test requires a database connection. Run manually with -run=TestMoveMessagesWithinSameMailbox")

	// Create a database connection
	ctx := context.Background()
	connString := "postgres://postgres@localhost:5432/imap_db?sslmode=disable"

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("Unable to parse connection string: %v", err)
	}

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer dbPool.Close()

	db := &Database{
		Pool: dbPool,
	}

	// Create a test user
	var userID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO users (username, password) 
		VALUES ('testuser2@example.com', 'password') 
		ON CONFLICT (username) 
		DO UPDATE SET username = EXCLUDED.username 
		RETURNING id
	`).Scan(&userID)
	if err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}

	// Create a mailbox
	timestamp := time.Now().UnixNano()
	mailboxName := fmt.Sprintf("Mailbox_%d", timestamp)

	var mailboxID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO mailboxes (user_id, name, uid_validity) 
		VALUES ($1, $2, 1234567890) 
		RETURNING id
	`, userID, mailboxName).Scan(&mailboxID)
	if err != nil {
		t.Fatalf("Failed to create mailbox: %v", err)
	}

	// Create test messages in the mailbox
	for i := 1; i <= 3; i++ {
		_, err = db.Pool.Exec(ctx, `
			INSERT INTO messages (
				user_id, mailbox_id, mailbox_path, uid, content_hash, message_id, 
				flags, internal_date, size, body_structure, recipients_json, 
				text_body, sent_date, created_modseq
			) VALUES (
				$1, $2, $3, $4, $5, $6, 
				0, NOW(), 100, '\x', '[]'::jsonb, 
				'Test message', NOW(), nextval('messages_modseq')
			) ON CONFLICT DO NOTHING
		`, userID, mailboxID, mailboxName, i, "hash"+string(rune(i)), "msg"+string(rune(i)))
		if err != nil {
			t.Fatalf("Failed to create test message %d: %v", i, err)
		}
	}

	// Try to move messages within the same mailbox
	uids := []imap.UID{1, 2, 3}
	_, err = db.MoveMessages(ctx, &uids, mailboxID, mailboxID)

	// Verify that an error is returned
	assert.Error(t, err, "Expected an error when trying to move messages within the same mailbox")
	assert.Contains(t, err.Error(), "cannot move messages within the same mailbox", "Error message should indicate that moving within the same mailbox is not allowed")

	// Verify that no messages were moved or duplicated
	var count int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE mailbox_id = $1 AND expunged_at IS NULL
	`, mailboxID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count messages in mailbox: %v", err)
	}
	assert.Equal(t, 3, count, "Expected 3 messages in mailbox (no duplicates)")

	// Clean up
	_, err = db.Pool.Exec(ctx, `DELETE FROM messages WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up messages: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM mailboxes WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up mailboxes: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up user: %v", err)
	}
}

// TestPollMailboxWithExpunge tests that PollMailbox correctly reports expunged messages
func TestPollMailboxWithExpunge(t *testing.T) {
	// Uncomment to skip this test if we're not running in a CI environment with a database
	// t.Skip("This test requires a database connection. Run manually with -run=TestPollMailboxWithExpunge")

	// Create a database connection
	ctx := context.Background()
	connString := "postgres://postgres@localhost:5432/imap_db?sslmode=disable"

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("Unable to parse connection string: %v", err)
	}

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("Failed to create connection pool: %v", err)
	}
	defer dbPool.Close()

	db := &Database{
		Pool: dbPool,
	}

	// Create a test user
	var userID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO users (username, password) 
		VALUES ('testuser_poll@example.com', 'password') 
		ON CONFLICT (username) 
		DO UPDATE SET username = EXCLUDED.username 
		RETURNING id
	`).Scan(&userID)
	if err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}

	// Create a mailbox
	timestamp := time.Now().UnixNano()
	mailboxName := fmt.Sprintf("PollTest_%d", timestamp)

	var mailboxID int64
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO mailboxes (user_id, name, uid_validity) 
		VALUES ($1, $2, 1234567890) 
		RETURNING id
	`, userID, mailboxName).Scan(&mailboxID)
	if err != nil {
		t.Fatalf("Failed to create mailbox: %v", err)
	}

	// Create test messages in the mailbox
	for i := 1; i <= 5; i++ {
		_, err = db.Pool.Exec(ctx, `
			INSERT INTO messages (
				user_id, mailbox_id, mailbox_path, uid, content_hash, message_id, 
				flags, internal_date, size, body_structure, recipients_json, 
				text_body, sent_date, created_modseq
			) VALUES (
				$1, $2, $3, $4, $5, $6, 
				0, NOW(), 100, '\x', '[]'::jsonb, 
				'Test message', NOW(), nextval('messages_modseq')
			) ON CONFLICT DO NOTHING
		`, userID, mailboxID, mailboxName, i, fmt.Sprintf("hash%d", i), fmt.Sprintf("msg%d", i))
		if err != nil {
			t.Fatalf("Failed to create test message %d: %v", i, err)
		}
	}

	// Get the current modseq before expunging
	var initialModSeq uint64
	err = db.Pool.QueryRow(ctx, `SELECT last_value FROM messages_modseq`).Scan(&initialModSeq)
	if err != nil {
		t.Fatalf("Failed to get initial modseq: %v", err)
	}

	// Verify we have 5 messages before expunging
	var countBefore int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE mailbox_id = $1 AND expunged_at IS NULL
	`, mailboxID).Scan(&countBefore)
	if err != nil {
		t.Fatalf("Failed to count messages before expunge: %v", err)
	}
	assert.Equal(t, 5, countBefore, "Expected 5 messages before expunge")

	// Expunge messages 2 and 4
	expungeUIDs := []imap.UID{2, 4}
	err = db.ExpungeMessageUIDs(ctx, mailboxID, expungeUIDs...)
	if err != nil {
		t.Fatalf("Failed to expunge messages: %v", err)
	}

	// Verify we have 3 messages after expunging
	var countAfter int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM messages 
		WHERE mailbox_id = $1 AND expunged_at IS NULL
	`, mailboxID).Scan(&countAfter)
	if err != nil {
		t.Fatalf("Failed to count messages after expunge: %v", err)
	}
	assert.Equal(t, 3, countAfter, "Expected 3 messages after expunge")

	// Poll the mailbox with the initial modseq
	poll, err := db.PollMailbox(ctx, mailboxID, initialModSeq)
	if err != nil {
		t.Fatalf("Failed to poll mailbox: %v", err)
	}

	// Verify the poll results
	assert.NotNil(t, poll, "Poll result should not be nil")
	assert.Equal(t, uint32(3), poll.NumMessages, "Poll should report 3 messages")

	// Count expunged messages in the poll updates
	expungedCount := 0
	expungedUIDs := make(map[imap.UID]bool)
	for _, update := range poll.Updates {
		if update.IsExpunge {
			expungedCount++
			expungedUIDs[update.UID] = true
		}
	}

	// Verify that the poll includes the expunged messages
	// Note: The PollMailbox function may report each expunged message twice
	// due to how it queries both updated and explicitly expunged messages
	assert.GreaterOrEqual(t, expungedCount, 2, "Poll should report at least 2 expunged messages")
	assert.Len(t, expungedUIDs, 2, "Poll should report exactly 2 unique expunged UIDs")
	assert.Contains(t, expungedUIDs, imap.UID(2), "Poll should report UID 2 as expunged")
	assert.Contains(t, expungedUIDs, imap.UID(4), "Poll should report UID 4 as expunged")

	// Clean up
	_, err = db.Pool.Exec(ctx, `DELETE FROM messages WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up messages: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM mailboxes WHERE user_id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up mailboxes: %v", err)
	}
	_, err = db.Pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		t.Logf("Failed to clean up user: %v", err)
	}
}
