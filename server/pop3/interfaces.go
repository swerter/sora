package pop3

import (
	"context"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/testutils"
)

// Import the interfaces from testutils
type S3StorageInterface = testutils.S3StorageInterface
type UploadWorkerInterface = testutils.UploadWorkerInterface
type CacheInterface = testutils.CacheInterface

// DBer is an interface for database operations specific to POP3
type DBer interface {
	GetUserIDByAddress(ctx context.Context, address string) (int64, error)
	GetMailboxByName(ctx context.Context, userID int64, name string) (*db.DBMailbox, error)
	Authenticate(ctx context.Context, userID int64, password string) error
	GetMailboxMessageCountAndSizeSum(ctx context.Context, mailboxID int64) (int, int64, error)
	ListMessages(ctx context.Context, mailboxID int64) ([]db.Message, error)
	ExpungeMessageUIDs(ctx context.Context, mailboxID int64, uids ...imap.UID) error
	Close()
}
