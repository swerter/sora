package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/migadu/sora/consts"
	"golang.org/x/crypto/bcrypt"
)

// Authenticate verifies the provided username and password against the records
// in the `passwords` table. If successful, it returns the associated `user_id`.
func (db *Database) Authenticate(ctx context.Context, username string, password string) (int64, error) {
	var hashedPassword string
	var userID int64

	normalizedUsername := strings.ToLower(strings.TrimSpace(username))
	if normalizedUsername == "" {
		return 0, errors.New("username cannot be empty")
	}
	if password == "" {
		return 0, errors.New("password cannot be empty")
	}

	err := db.Pool.QueryRow(ctx,
		"SELECT user_id, password FROM passwords WHERE username = $1",
		normalizedUsername).Scan(&userID, &hashedPassword)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Username (identity) not found in the passwords table
			return 0, consts.ErrUserNotFound
		}
		// Log other unexpected database errors
		log.Printf("error fetching credentials for username %s: %v", normalizedUsername, err)
		return 0, fmt.Errorf("database error during authentication: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password)); err != nil {
		// Password does not match
		return 0, errors.New("invalid password")
	}

	// Authentication successful
	return userID, nil
}

// GetUserIDByAddress retrieves the main user ID associated with a given identity (username/address)
// by looking it up in the `passwords` table.
func (db *Database) GetUserIDByAddress(ctx context.Context, username string) (int64, error) {
	var userID int64
	normalizedUsername := strings.ToLower(strings.TrimSpace(username))

	if normalizedUsername == "" {
		return 0, errors.New("username cannot be empty")
	}

	// Query the passwords table for the user_id associated with the username
	err := db.Pool.QueryRow(ctx, "SELECT user_id FROM passwords WHERE username = $1", normalizedUsername).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Identity (username) not found in the passwords table
			return 0, consts.ErrUserNotFound
		}
		log.Printf("error fetching user ID for username %s: %v", normalizedUsername, err)
		return 0, fmt.Errorf("database error fetching user ID: %w", err)
	}
	return userID, nil
}
