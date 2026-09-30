//go:build integration

package resilient_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
	"github.com/migadu/sora/pkg/resilient"
	"github.com/stretchr/testify/require"
)

// softDeletedFixtureAccount creates an account with an INBOX holding n messages and
// soft-deletes it with deleted_at pinned to deletedAt. Its rows are removed on cleanup.
func softDeletedFixtureAccount(t *testing.T, rdb *resilient.ResilientDatabase, deletedAt time.Time, n int) int64 {
	t.Helper()
	ctx := context.Background()
	account := common.CreateTestAccount(t, rdb)
	accountID, err := rdb.GetAccountIDByAddressWithRetry(ctx, account.Email)
	require.NoError(t, err)
	inbox, err := rdb.GetMailboxByNameWithRetry(ctx, accountID, "INBOX")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		restoreFixture(t, rdb, accountID, inbox.ID, "INBOX", fmt.Sprintf("<harddelete-%d-%d@example.com>", accountID, i))
	}
	_, err = rdb.GetDatabase().GetWritePool().Exec(ctx, `UPDATE accounts SET deleted_at = $2 WHERE id = $1`, accountID, deletedAt)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx := context.Background()
		pool := rdb.GetDatabase().GetWritePool()
		for _, q := range []string{
			`DELETE FROM messages WHERE account_id = $1`,
			`DELETE FROM pending_uploads WHERE account_id = $1`,
			`DELETE FROM mailboxes WHERE account_id = $1`,
			`DELETE FROM credentials WHERE account_id = $1`,
			`DELETE FROM accounts WHERE id = $1`,
		} {
			if _, err := pool.Exec(ctx, q, accountID); err != nil {
				t.Errorf("cleanup of account %d: %s: %v", accountID, q, err)
			}
		}
	})
	return accountID
}

func accountCounts(t *testing.T, rdb *resilient.ResilientDatabase, accountID int64) (live, mailboxes int) {
	t.Helper()
	require.NoError(t, rdb.GetDatabase().GetReadPool().QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM messages WHERE account_id = $1 AND expunged_at IS NULL),
		       (SELECT count(*) FROM mailboxes WHERE account_id = $1)`, accountID).Scan(&live, &mailboxes))
	return live, mailboxes
}

// A soft-deleted account is hard-deleted in bounded steps, each its own transaction, so a
// large account drains within the write deadline instead of being retried whole every
// cycle. The batch listing then no longer offers a finished account, so it cannot hold
// one of the batch's slots while it waits for its messages to be reaped and finalized.
func TestCleanupSoftDeletedAccounts_ChunkedPerAccount(t *testing.T) {
	rdb := common.SetupTestDatabase(t)
	ctx := context.Background()

	// deleted_at earlier than any account already in the shared test database, so a
	// grace period ending just after it selects only this test's accounts.
	marker := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	var earliest *time.Time
	require.NoError(t, rdb.GetDatabase().GetReadPool().QueryRow(ctx, `SELECT min(deleted_at) FROM accounts`).Scan(&earliest))
	if earliest != nil && earliest.Before(marker) {
		marker = earliest.Add(-24 * time.Hour)
	}
	gracePeriod := time.Since(marker.Add(time.Minute))

	big := softDeletedFixtureAccount(t, rdb, marker, 5)
	small := softDeletedFixtureAccount(t, rdb, marker.Add(time.Second), 1)

	// Batch size 2 over 5 messages: three expunge steps and a final step, four transactions.
	require.NoError(t, rdb.HardDeleteAccountChunkedForTest(ctx, big, 2))
	live, mailboxes := accountCounts(t, rdb, big)
	require.Equal(t, 0, live, "every message of the account is expunged")
	require.Equal(t, 0, mailboxes, "the last step removes the mailboxes")

	// The public path lists what is left (only the small account: the big one has no
	// mailboxes any more) and finishes it.
	done, err := rdb.CleanupSoftDeletedAccountsWithRetry(ctx, gracePeriod)
	require.NoError(t, err)
	require.Equal(t, int64(1), done, "only the account still holding mailboxes is processed")
	live, mailboxes = accountCounts(t, rdb, small)
	require.Equal(t, 0, live)
	require.Equal(t, 0, mailboxes)

	done, err = rdb.CleanupSoftDeletedAccountsWithRetry(ctx, gracePeriod)
	require.NoError(t, err)
	require.Equal(t, int64(0), done, "finished accounts are not listed again")
}
