package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTriggerTestMailbox creates an account and mailbox for trigger testing.
// Returns the database, account ID, mailbox ID, and mailbox name.
func setupTriggerTestMailbox(t *testing.T) (*Database, int64, int64, string) {
	t.Helper()
	db := setupTestDatabase(t)
	ctx := context.Background()

	testEmail := fmt.Sprintf("test_trigger_%s_%d@example.com", t.Name(), time.Now().UnixNano())

	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	req := CreateAccountRequest{Email: testEmail, Password: "password", IsPrimary: true, HashType: "bcrypt"}
	_, err = db.CreateAccount(ctx, tx, req)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	accountID, err := db.GetAccountIDByAddress(ctx, testEmail)
	require.NoError(t, err)

	mailboxName := "TestTriggerBox"
	tx, err = db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	err = db.CreateMailbox(ctx, tx, accountID, mailboxName, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	mailbox, err := db.GetMailboxByName(ctx, accountID, mailboxName)
	require.NoError(t, err)

	return db, accountID, mailbox.ID, mailboxName
}

// insertTestMsg is a concise helper to insert a message with a given UID.
func insertTestMsg(t *testing.T, db *Database, ctx context.Context, accountID, mailboxID int64, mailboxName string, uid uint32, flags []imap.Flag) {
	t.Helper()
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	insertTestMessageWithUIDForTrigger(t, db, ctx, tx, accountID, mailboxID, mailboxName, uid, flags)
	require.NoError(t, tx.Commit(ctx))
}

// insertTestMessageWithUIDForTrigger inserts a test message with a given UID within a transaction.
func insertTestMessageWithUIDForTrigger(t *testing.T, db *Database, ctx context.Context, tx pgx.Tx, accountID, mailboxID int64, mailboxName string, uid uint32, flags []imap.Flag) {
	t.Helper()
	defaultBodyStructure := &imap.BodyStructureSinglePart{
		Type:    "text",
		Subtype: "plain",
		Size:    512,
	}
	var bs imap.BodyStructure = defaultBodyStructure
	now := time.Now()
	options := &InsertMessageOptions{
		AccountID:     accountID,
		MailboxID:     mailboxID,
		MailboxName:   mailboxName,
		S3Domain:      "example.com",
		S3Localpart:   fmt.Sprintf("test/trigger/%s/%d", t.Name(), uid),
		ContentHash:   fmt.Sprintf("trigger_%s_%d", t.Name(), uid),
		MessageID:     fmt.Sprintf("<trigger_%s_%d@example.com>", t.Name(), uid),
		Flags:         flags,
		InternalDate:  now,
		Size:          512,
		Subject:       fmt.Sprintf("Trigger Test %d", uid),
		PlaintextBody: "body",
		RawHeaders:    "headers",
		SentDate:      now,
		PreservedUID:  &uid,
		BodyStructure: &bs,
	}
	_, _, err := db.InsertMessageFromImporter(ctx, tx, options)
	require.NoError(t, err, "failed to insert test message with UID %d", uid)
}

// getSeqNums returns a map of uid -> seqnum for a mailbox from message_sequences.
func getSeqNums(t *testing.T, db *Database, ctx context.Context, mailboxID int64) map[int64]int {
	t.Helper()
	rows, err := db.GetReadPool().Query(ctx,
		"SELECT uid, seqnum FROM message_sequences WHERE mailbox_id = $1 ORDER BY seqnum", mailboxID)
	require.NoError(t, err)
	defer rows.Close()

	result := make(map[int64]int)
	for rows.Next() {
		var uid int64
		var seqnum int
		require.NoError(t, rows.Scan(&uid, &seqnum))
		result[uid] = seqnum
	}
	require.NoError(t, rows.Err())
	return result
}

// getCustomFlagsCache reads the custom_flags_cache column from mailbox_stats.
func getCustomFlagsCache(t *testing.T, db *Database, ctx context.Context, mailboxID int64) ([]string, bool) {
	t.Helper()
	var cacheJSON []byte
	err := db.GetReadPool().QueryRow(ctx,
		"SELECT custom_flags_cache FROM mailbox_stats WHERE mailbox_id = $1", mailboxID).Scan(&cacheJSON)
	if err == pgx.ErrNoRows || cacheJSON == nil {
		return nil, false
	}
	require.NoError(t, err)

	var flags []string
	require.NoError(t, json.Unmarshal(cacheJSON, &flags))
	return flags, true
}

// cleanupMailbox removes all messages from a mailbox.
func cleanupMailbox(t *testing.T, db *Database, ctx context.Context, mailboxID int64) {
	t.Helper()
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "DELETE FROM messages WHERE mailbox_id = $1", mailboxID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

// ============================================================================
// SEQUENCE TRIGGER TESTS
// ============================================================================

func TestSequenceTrigger_SingleMonotonicInsert_FastPath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert messages with monotonically increasing UIDs (fast path)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 20, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 30, nil)

	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 3, len(seqs), "Should have 3 sequence entries")
	assert.Equal(t, 1, seqs[10], "UID 10 should have seqnum 1")
	assert.Equal(t, 2, seqs[20], "UID 20 should have seqnum 2")
	assert.Equal(t, 3, seqs[30], "UID 30 should have seqnum 3")

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestSequenceTrigger_NonMonotonicInsert_FullRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert messages with monotonic UIDs first
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 100, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 200, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 300, nil)

	// Now insert a message with a UID that's LOWER (simulates import with --preserve-uids)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 50, nil)

	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 4, len(seqs), "Should have 4 sequence entries")
	// After full rebuild, sequence numbers should be ordered by UID
	assert.Equal(t, 1, seqs[50], "UID 50 should have seqnum 1 (lowest UID)")
	assert.Equal(t, 2, seqs[100], "UID 100 should have seqnum 2")
	assert.Equal(t, 3, seqs[200], "UID 200 should have seqnum 3")
	assert.Equal(t, 4, seqs[300], "UID 300 should have seqnum 4")

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestSequenceTrigger_NonMonotonicGapFill(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert with gaps: 1, 2, 3, then 100, 101, then fill gap with 50, 51, 52
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 1, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 2, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 3, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 100, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 101, nil)

	// Now fill the gap
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 50, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 51, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 52, nil)

	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 8, len(seqs), "Should have 8 sequence entries")

	// Verify proper ordering by UID
	assert.Equal(t, 1, seqs[1])
	assert.Equal(t, 2, seqs[2])
	assert.Equal(t, 3, seqs[3])
	assert.Equal(t, 4, seqs[50])
	assert.Equal(t, 5, seqs[51])
	assert.Equal(t, 6, seqs[52])
	assert.Equal(t, 7, seqs[100])
	assert.Equal(t, 8, seqs[101])

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestSequenceTrigger_BulkInsert_FullRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Bulk insert (multi-message in one transaction triggers full rebuild)
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	insertTestMessageWithUIDForTrigger(t, db, ctx, tx, accountID, mailboxID, mailboxName, 10, nil)
	insertTestMessageWithUIDForTrigger(t, db, ctx, tx, accountID, mailboxID, mailboxName, 20, nil)
	insertTestMessageWithUIDForTrigger(t, db, ctx, tx, accountID, mailboxID, mailboxName, 30, nil)
	require.NoError(t, tx.Commit(ctx))

	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 3, len(seqs))
	assert.Equal(t, 1, seqs[10])
	assert.Equal(t, 2, seqs[20])
	assert.Equal(t, 3, seqs[30])

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestSequenceTrigger_ExpungeCausesRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 20, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 30, nil)

	// Expunge the middle message
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, err = db.ExpungeMessageUIDs(ctx, tx, mailboxID, 20)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// After expunge, sequences should be renumbered
	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 2, len(seqs), "Should have 2 sequence entries after expunge")
	assert.Equal(t, 1, seqs[10], "UID 10 should have seqnum 1")
	assert.Equal(t, 2, seqs[30], "UID 30 should have seqnum 2 (renumbered after expunge)")

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestSequenceTrigger_MonotonicAfterExpunge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, nil)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 20, nil)

	// Expunge
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, err = db.ExpungeMessageUIDs(ctx, tx, mailboxID, 20)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Insert new message with higher UID (fast path should work)
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 30, nil)

	seqs := getSeqNums(t, db, ctx, mailboxID)
	assert.Equal(t, 2, len(seqs))
	assert.Equal(t, 1, seqs[10])
	assert.Equal(t, 2, seqs[30])

	cleanupMailbox(t, db, ctx, mailboxID)
}

// ============================================================================
// CUSTOM FLAGS CACHE TRIGGER TESTS
// ============================================================================

func TestCustomFlagsCache_EmptyOnNewMailbox(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, _, mailboxID, _ := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// New mailbox with no messages should have NULL cache (not yet populated)
	// OR '[]' if the trigger initialized it
	flags, hasCacheRow := getCustomFlagsCache(t, db, ctx, mailboxID)
	if hasCacheRow {
		assert.Empty(t, flags, "New mailbox should have empty custom flags cache")
	}
	// NULL is also acceptable (no mailbox_stats row yet)
}

func TestCustomFlagsCache_PopulatedOnInsertWithCustomFlags(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert message with custom flags
	customFlags := []imap.Flag{imap.FlagSeen, imap.Flag("Important"), imap.Flag("Work")}
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, customFlags)

	cache, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.True(t, hasCache, "Should have cache after inserting message with custom flags")
	assert.Contains(t, cache, "Important")
	assert.Contains(t, cache, "Work")
	// System flags should NOT be in cache
	for _, f := range cache {
		assert.NotEqual(t, "\\Seen", f, "System flags should not appear in custom flags cache")
	}

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestCustomFlagsCache_NotRebuiltForSystemFlagsOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert message with only system flags (no custom flags)
	systemFlags := []imap.Flag{imap.FlagSeen, imap.FlagFlagged}
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, systemFlags)

	// Cache should be empty (or null) since no custom flags
	cache, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)
	if hasCache {
		assert.Empty(t, cache, "Cache should be empty when only system flags exist")
	}

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestCustomFlagsCache_UpdatedOnFlagChange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert message with no custom flags
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10, nil)

	// Add custom flags
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, _, err = db.SetMessageFlags(ctx, tx, 10, mailboxID, []imap.Flag{imap.FlagSeen, imap.Flag("Project-X")})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	cache, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.True(t, hasCache)
	assert.Contains(t, cache, "Project-X")

	// Now change to different custom flags
	tx, err = db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, _, err = db.SetMessageFlags(ctx, tx, 10, mailboxID, []imap.Flag{imap.FlagSeen, imap.Flag("Project-Y")})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	cache, hasCache = getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.True(t, hasCache)
	assert.Contains(t, cache, "Project-Y")
	assert.NotContains(t, cache, "Project-X", "Old custom flag should be removed from cache")

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestCustomFlagsCache_ClearedOnExpunge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert message with custom flags
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10,
		[]imap.Flag{imap.Flag("CustomFlag1")})

	cache, _ := getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.Contains(t, cache, "CustomFlag1")

	// Expunge the message
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, err = db.ExpungeMessageUIDs(ctx, tx, mailboxID, 10)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Cache should now be empty
	cache, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)
	if hasCache {
		assert.Empty(t, cache, "Cache should be empty after expunging the only message with custom flags")
	}

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestCustomFlagsCache_MultipleMessagesAggregated(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert messages with different custom flags
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10,
		[]imap.Flag{imap.Flag("Alpha"), imap.Flag("Beta")})
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 20,
		[]imap.Flag{imap.Flag("Beta"), imap.Flag("Gamma")})

	cache, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.True(t, hasCache)
	assert.Contains(t, cache, "Alpha")
	assert.Contains(t, cache, "Beta")
	assert.Contains(t, cache, "Gamma")

	// Expunge message with Alpha+Beta, only Gamma (from message 20) should remain
	// But Beta is shared so it should also remain
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	_, err = db.ExpungeMessageUIDs(ctx, tx, mailboxID, 10)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	cache, hasCache = getCustomFlagsCache(t, db, ctx, mailboxID)
	assert.True(t, hasCache)
	assert.NotContains(t, cache, "Alpha", "Alpha should be gone (only on expunged message)")
	assert.Contains(t, cache, "Beta", "Beta should remain (on message UID 20)")
	assert.Contains(t, cache, "Gamma", "Gamma should remain (on message UID 20)")

	cleanupMailbox(t, db, ctx, mailboxID)
}

func TestCustomFlagsCache_MatchesGoFunction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db, accountID, mailboxID, mailboxName := setupTriggerTestMailbox(t)
	defer db.Close()
	ctx := context.Background()

	// Insert messages with various custom flags
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 10,
		[]imap.Flag{imap.FlagSeen, imap.Flag("Todo"), imap.Flag("Urgent")})
	insertTestMsg(t, db, ctx, accountID, mailboxID, mailboxName, 20,
		[]imap.Flag{imap.Flag("Urgent"), imap.Flag("Archive")})

	// Get flags via Go function (which reads from cache)
	goFlags, err := db.GetUniqueCustomFlagsForMailbox(ctx, mailboxID)
	require.NoError(t, err)

	// Get flags directly from cache
	cacheFlags, hasCache := getCustomFlagsCache(t, db, ctx, mailboxID)

	if hasCache && len(cacheFlags) > 0 {
		// Both should contain the same flags (order may differ)
		assert.ElementsMatch(t, cacheFlags, goFlags,
			"Go function result should match cached flags")
	}

	// Verify expected flags
	assert.Contains(t, goFlags, "Todo")
	assert.Contains(t, goFlags, "Urgent")
	assert.Contains(t, goFlags, "Archive")

	cleanupMailbox(t, db, ctx, mailboxID)
}
