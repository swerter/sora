//go:build integration

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/migadu/sora/consts"
)

// TestImportSieveScript_LeavesBrokenScriptInactive: a script delivery cannot
// compile is skipped whole at delivery, so import must never activate one,
// nor overwrite a working script with it. It is still stored, under its own
// name, for the user to see and fix, and the mail import goes on. This is how
// a Dovecot script requiring an extension go-sieve lacked reached production
// as the active script.
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

	runImport := func(t *testing.T, script string, extensions []string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "dovecot.sieve")
		if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
			t.Fatal(err)
		}
		importer, err := NewImporter(ctx, maildir, email, 1, rdb, nil, ImporterOptions{
			CleanupDB:       true,
			TestMode:        true,
			SievePath:       path,
			SieveExtensions: extensions,
		})
		if err != nil {
			t.Fatalf("NewImporter: %v", err)
		}
		defer importer.Close()
		if err := importer.Run(); err != nil {
			t.Fatalf("import: %v", err)
		}
	}
	good := "require [\"fileinto\"];\nif header :contains \"subject\" \"x\" { fileinto \"X\"; }\n"
	broken := "require [\"enclose\"];\nkeep;\n"

	t.Run("good script is imported and activated", func(t *testing.T) {
		runImport(t, good, nil)
		active, err := rdb.GetActiveScriptWithRetry(ctx, accountID)
		if err != nil {
			t.Fatalf("no active script after importing a valid one: %v", err)
		}
		if active.Name != "imported" {
			t.Fatalf("active script %q, want \"imported\"", active.Name)
		}
	})

	t.Run("broken script is stored inactive and the working one stays active", func(t *testing.T) {
		runImport(t, broken, nil)
		active, err := rdb.GetActiveScriptWithRetry(ctx, accountID)
		if err != nil {
			t.Fatalf("the working script lost its active state: %v", err)
		}
		if active.Name != "imported" || active.Script != good {
			t.Fatalf("active script %q with content %q; the previous working script must stay", active.Name, active.Script)
		}
		stored, err := rdb.GetScriptByNameWithRetry(ctx, "imported-invalid", accountID)
		if err != nil {
			t.Fatalf("broken script was not stored for the user to fix: %v", err)
		}
		if stored.Active || stored.Script != broken {
			t.Fatalf("stored active=%v content=%q, want inactive with the imported content", stored.Active, stored.Script)
		}
	})

	t.Run("configured extensions are honoured", func(t *testing.T) {
		// editheader is opt-in: with it configured the script compiles and is
		// activated; without it, it would be left inactive.
		script := "require [\"editheader\"];\naddheader \"X-Imported\" \"yes\";\n"
		runImport(t, script, []string{"fileinto", "editheader"})
		active, err := rdb.GetActiveScriptWithRetry(ctx, accountID)
		if err != nil {
			t.Fatalf("script valid under the configured set was not activated: %v", err)
		}
		if active.Script != script {
			t.Fatalf("active script content %q, want the imported editheader script", active.Script)
		}
	})

	t.Run("no active script when only a broken one was ever imported", func(t *testing.T) {
		fresh := fmt.Sprintf("sieve-import-fresh-%d@example.com", time.Now().UnixNano())
		freshID := createSieveTestAccount(t, rdb, fresh, "password123")
		path := filepath.Join(t.TempDir(), "dovecot.sieve")
		if err := os.WriteFile(path, []byte(broken), 0o644); err != nil {
			t.Fatal(err)
		}
		importer, err := NewImporter(ctx, maildir, fresh, 1, rdb, nil, ImporterOptions{CleanupDB: true, TestMode: true, SievePath: path})
		if err != nil {
			t.Fatal(err)
		}
		defer importer.Close()
		if err := importer.Run(); err != nil {
			t.Fatal(err)
		}
		if _, err := rdb.GetActiveScriptWithRetry(ctx, freshID); !errors.Is(err, consts.ErrDBNotFound) {
			t.Fatalf("want no active script (ErrDBNotFound), got err=%v", err)
		}
	})
}
