package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unique suffix per test run to avoid collisions with existing data
var testRunID = fmt.Sprintf("%d", time.Now().UnixNano()%1000000)

// TestCreateAccountWithLengthLimits verifies that account creation enforces email length limits.
// Note: local part limit is 128 (not RFC 5321's 64) to accommodate SRS-rewritten addresses.
func TestCreateAccountWithLengthLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		email   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid email address",
			email:   "lentest" + testRunID + "@example.com",
			wantErr: false,
		},
		{
			name:    "local part at RFC 5321 length (64 chars) - allowed",
			email:   strings.Repeat("a", 64-len(testRunID)) + testRunID + "@example.com",
			wantErr: false,
		},
		{
			name:    "SRS-length local part (80 chars) - allowed",
			email:   strings.Repeat("b", 80-len(testRunID)) + testRunID + "@example.com",
			wantErr: false,
		},
		{
			name:    "local part at max length (128 chars)",
			email:   strings.Repeat("c", 128-len(testRunID)) + testRunID + "@example.com",
			wantErr: false,
		},
		{
			name:    "local part exceeds max (129 chars)",
			email:   strings.Repeat("a", 129) + "@example.com",
			wantErr: true,
			errMsg:  "local part exceeds maximum length of 128 characters",
		},
		{
			name:    "domain at max length (255 chars)",
			email:   "d" + testRunID + "@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 59) + ".abc",
			wantErr: false,
		},
		{
			name:    "domain exceeds max (256 chars)",
			email:   "user@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 60) + ".abc",
			wantErr: true,
			errMsg:  "domain exceeds maximum length of 255 characters",
		},
		{
			name:    "total at max length (384 chars)",
			email:   strings.Repeat("e", 128-len(testRunID)) + testRunID + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "." + strings.Repeat("e", 59) + ".abc",
			wantErr: false,
		},
		{
			name:    "local part way over limit (1000 chars)",
			email:   strings.Repeat("x", 1000) + "@example.com",
			wantErr: true,
			errMsg:  "local part exceeds maximum length of 128 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := db.GetWritePool().Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)

			req := CreateAccountRequest{
				Email:     tt.email,
				Password:  "password123",
				IsPrimary: true,
				HashType:  "bcrypt",
			}

			accountID, err := db.CreateAccount(ctx, tx, req)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Equal(t, int64(0), accountID)
				tx.Rollback(ctx)
			} else {
				assert.NoError(t, err)
				assert.Greater(t, accountID, int64(0))
				err = tx.Commit(ctx)
				require.NoError(t, err)

				// Verify account was created
				result, err := db.AccountExists(ctx, tt.email)
				assert.NoError(t, err)
				assert.True(t, result.Exists)
			}
		})
	}
}

// TestAddCredentialWithLengthLimits verifies that adding credentials enforces email length limits
func TestAddCredentialWithLengthLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping database integration test in short mode")
	}

	db := setupTestDatabase(t)
	defer db.Close()

	ctx := context.Background()

	// Create a base account first with unique name
	tx, err := db.GetWritePool().Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	baseEmail := "credbase" + testRunID + "@example.com"
	baseReq := CreateAccountRequest{
		Email:     baseEmail,
		Password:  "password123",
		IsPrimary: true,
		HashType:  "bcrypt",
	}
	accountID, err := db.CreateAccount(ctx, tx, baseReq)
	require.NoError(t, err)
	require.Greater(t, accountID, int64(0))

	err = tx.Commit(ctx)
	require.NoError(t, err)

	tests := []struct {
		name    string
		email   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid alias email",
			email:   "credalias" + testRunID + "@example.com",
			wantErr: false,
		},
		{
			name:    "alias with local part exceeding max",
			email:   strings.Repeat("a", 129) + "@example.com",
			wantErr: true,
			errMsg:  "local part exceeds maximum length of 128 characters",
		},
		{
			name:    "alias with domain exceeding max",
			email:   "user@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 60) + ".abc",
			wantErr: true,
			errMsg:  "domain exceeds maximum length of 255 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx2, err := db.GetWritePool().Begin(ctx)
			require.NoError(t, err)
			defer tx2.Rollback(ctx)

			addReq := AddCredentialRequest{
				AccountID:   accountID,
				NewEmail:    tt.email,
				NewPassword: "password456",
				IsPrimary:   false,
				NewHashType: "bcrypt",
			}

			err = db.AddCredential(ctx, tx2, addReq)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				tx2.Rollback(ctx)
			} else {
				assert.NoError(t, err)
				err = tx2.Commit(ctx)
				require.NoError(t, err)

				// Verify credential was added
				creds, err := db.ListCredentials(ctx, tt.email)
				assert.NoError(t, err)
				assert.NotEmpty(t, creds)
			}
		})
	}
}
