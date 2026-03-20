package db

import (
	"errors"
	"strings"
)

// Sentinel errors for database operations
var (
	// ErrAccountNotFound indicates that an account was not found in the database
	ErrAccountNotFound = errors.New("account not found")

	// ErrMailboxNotFound indicates that a mailbox was not found in the database
	ErrMailboxNotFound = errors.New("mailbox not found")

	// ErrMessageNotFound indicates that a message was not found in the database
	ErrMessageNotFound = errors.New("message not found")

	// ErrInvalidCredentials indicates that the provided credentials are invalid
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrDuplicateAccount indicates that an account with the given email already exists
	ErrDuplicateAccount = errors.New("account already exists")

	// ErrDuplicateMailbox indicates that a mailbox with the given name already exists
	ErrDuplicateMailbox = errors.New("mailbox already exists")

	// Connection pool errors
	ErrNoConnections     = errors.New("database connection pool exhausted")
	ErrConnectionRefused = errors.New("database connection refused")
	ErrTooManyClients    = errors.New("database has too many clients")
	ErrMaxClientConn     = errors.New("database max_client_conn exceeded")

	// Connection errors
	ErrFailedToConnect = errors.New("failed to connect to database")
)

// ClassifyDBError inspects a raw database/pgx error and, if it matches a known
// transient connection pattern, wraps it with the appropriate sentinel so that
// errors.Is() works throughout the call chain. If the error doesn't match any
// known pattern, it is returned unchanged.
//
// This bridges the gap between pgx (which returns plain errors with descriptive
// messages) and our sentinel-based error handling.
func ClassifyDBError(err error) error {
	if err == nil {
		return nil
	}

	msg := err.Error()
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "no more connections") ||
		strings.Contains(lower, "connection pool exhausted"):
		return errors.Join(ErrNoConnections, err)

	case strings.Contains(lower, "connection refused"):
		return errors.Join(ErrConnectionRefused, err)

	case strings.Contains(lower, "too many clients"):
		return errors.Join(ErrTooManyClients, err)

	case strings.Contains(lower, "max_client_conn"):
		return errors.Join(ErrMaxClientConn, err)

	case strings.Contains(lower, "failed to connect"):
		return errors.Join(ErrFailedToConnect, err)
	}

	return err
}
