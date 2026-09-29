//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// An import must honour fts_retention the way delivery does: a message sent before the
// retention window is imported in full but gets no search row, since the cleaner would only
// prune that row again. Migrating years of old mail must not grow messages_fts_v2 with vectors
// nobody is meant to search. A zero retention indexes everything, as before.
func TestImporter_FTSRetentionSkipsOldMessages(t *testing.T) {
	if os.Getenv("SKIP_DB_TESTS") == "true" {
		t.Skip("Skipping database tests")
	}
	rdb := setupSimpleTestDatabase(t)
	defer rdb.Close()

	const window = 180 * 24 * time.Hour

	for _, tc := range []struct {
		name      string
		retention time.Duration
		batchTx   bool
		wantOld   bool
	}{
		{"batch path skips old mail", window, false, false},
		{"single-transaction path skips old mail", window, true, false},
		{"zero retention indexes old mail", 0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stamp := time.Now().UnixNano()
			email := fmt.Sprintf("ftsret%d@demo.com", stamp)
			createSimpleTestAccount(t, rdb, email, "testpassword123")

			maildir := filepath.Join(t.TempDir(), "Maildir")
			for _, sub := range []string{"cur", "new", "tmp"} {
				if err := os.MkdirAll(filepath.Join(maildir, sub), 0755); err != nil {
					t.Fatal(err)
				}
			}
			write := func(name, msgID string, sent time.Time, body string) {
				t.Helper()
				msg := fmt.Sprintf("Message-ID: <%s>\r\nDate: %s\r\nSubject: %s\r\n\r\n%s\r\n",
					msgID, sent.Format(time.RFC1123Z), name, body)
				if err := os.WriteFile(filepath.Join(maildir, "cur", name+":2,S"), []byte(msg), 0644); err != nil {
					t.Fatal(err)
				}
			}
			oldID := fmt.Sprintf("old-%d@test", stamp)
			recentID := fmt.Sprintf("recent-%d@test", stamp)
			write("old", oldID, time.Now().Add(-400*24*time.Hour), "an old body about quarterly reports")
			write("recent", recentID, time.Now().Add(-10*24*time.Hour), "a recent body about invoices")

			importer, err := NewImporter(context.Background(), maildir, email, 1, rdb, nil, ImporterOptions{
				PreserveFlags:        true,
				CleanupDB:            true,
				BatchSize:            10,
				BatchTransactionMode: tc.batchTx,
				TestMode:             true,
				FTSRetention:         tc.retention,
			})
			if err != nil {
				t.Fatalf("Failed to create importer: %v", err)
			}
			defer importer.Close()
			if err := importer.Run(); err != nil {
				t.Fatalf("Import failed: %v", err)
			}
			if importer.importedMessages != 2 {
				t.Fatalf("both messages must be imported whatever their age, got %d", importer.importedMessages)
			}

			hasSearchRow := func(msgID string) bool {
				t.Helper()
				var ok bool
				if err := rdb.QueryRowWithRetry(context.Background(), `
					SELECT EXISTS (SELECT 1 FROM messages m
					               JOIN messages_fts_v2 v ON v.content_hash = m.content_hash AND v.account_id = m.account_id
					               WHERE m.message_id = $1)`, msgID).Scan(&ok); err != nil {
					t.Fatal(err)
				}
				return ok
			}
			if !hasSearchRow(recentID) {
				t.Errorf("a message inside the retention window must get a search row")
			}
			if got := hasSearchRow(oldID); got != tc.wantOld {
				t.Errorf("message sent 400 days ago: search row = %v, want %v (retention %v)", got, tc.wantOld, tc.retention)
			}
		})
	}
}
