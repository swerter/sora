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

// TestImportSieveScript_LeavesBrokenScriptInactive: a script delivery cannot
// compile is skipped whole at delivery, so import must never activate one. It
// is still stored, so the user can see and fix it, and the mail import goes on.
// This is how a Dovecot script requiring an extension go-sieve lacked reached
// production as the active script.
func TestImportSieveScript_LeavesBrokenScriptInactive(t *testing.T) {
	if os.Getenv("SKIP_DB_TESTS") == "true" {
		t.Skip("Skipping database tests")
	}
	rdb := setupSieveTestDatabase(t)
	defer rdb.Close()
	ctx := context.Background()

	email := fmt.Sprintf("sieve-import-%d@example.com", time.Now().UnixNano())
	accountID := createSieveTestAccount(t, rdb, email, "password123")

	maildir := filepath.Join(t.TempDir(), "Maildir")
	for _, d := range []string{"cur", "new", "tmp"} {
		if err := os.MkdirAll(filepath.Join(maildir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	runImport := func(t *testing.T, script string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "dovecot.sieve")
		if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
			t.Fatal(err)
		}
		importer, err := NewImporter(ctx, maildir, email, 1, rdb, nil, ImporterOptions{
			CleanupDB: true,
			TestMode:  true,
			SievePath: path,
		})
		if err != nil {
			t.Fatalf("NewImporter: %v", err)
		}
		defer importer.Close()
		if err := importer.Run(); err != nil {
			t.Fatalf("import: %v", err)
		}
	}

	t.Run("good script is imported and activated", func(t *testing.T) {
		runImport(t, "require [\"fileinto\"];\nif header :contains \"subject\" \"x\" { fileinto \"X\"; }\n")
		active, err := rdb.GetActiveScriptWithRetry(ctx, accountID)
		if err != nil {
			t.Fatalf("no active script after importing a valid one: %v", err)
		}
		if active.Name != "imported" {
			t.Fatalf("active script %q, want \"imported\"", active.Name)
		}
	})

	t.Run("broken script replaces it but is left inactive", func(t *testing.T) {
		broken := "require [\"enclose\"];\nkeep;\n"
		runImport(t, broken)
		if _, err := rdb.GetActiveScriptWithRetry(ctx, accountID); err == nil {
			t.Fatal("a script that does not compile was activated; delivery would skip it whole")
		}
		stored, err := rdb.GetScriptByNameWithRetry(ctx, "imported", accountID)
		if err != nil {
			t.Fatalf("broken script was not stored for the user to fix: %v", err)
		}
		if stored.Script != broken {
			t.Fatalf("stored content %q, want the imported script", stored.Script)
		}
	})
}
