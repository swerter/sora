package pop3

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/testutils"
	"github.com/stretchr/testify/mock"
)

// Mock implementations for testing

// Mock implementations for testing

// MockDatabase is a mock implementation of the DBer interface
type MockDatabase struct {
	mock.Mock
}

// Ensure MockDatabase implements DBer
var _ DBer = (*MockDatabase)(nil)

func (m *MockDatabase) GetUserIDByAddress(ctx context.Context, address string) (int64, error) {
	args := m.Called(ctx, address)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDatabase) GetMailboxByName(ctx context.Context, userID int64, name string) (*db.DBMailbox, error) {
	args := m.Called(ctx, userID, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.DBMailbox), args.Error(1)
}

func (m *MockDatabase) Authenticate(ctx context.Context, userID int64, password string) error {
	args := m.Called(ctx, userID, password)
	return args.Error(0)
}

func (m *MockDatabase) GetMailboxMessageCountAndSizeSum(ctx context.Context, mailboxID int64) (int, int64, error) {
	args := m.Called(ctx, mailboxID)
	return args.Get(0).(int), args.Get(1).(int64), args.Error(2)
}

func (m *MockDatabase) ListMessages(ctx context.Context, mailboxID int64) ([]db.Message, error) {
	args := m.Called(ctx, mailboxID)
	return args.Get(0).([]db.Message), args.Error(1)
}

func (m *MockDatabase) ExpungeMessageUIDs(ctx context.Context, mailboxID int64, uids ...imap.UID) error {
	args := m.Called(ctx, mailboxID, uids)
	return args.Error(0)
}

func (m *MockDatabase) Close() {
	m.Called() // This method is called by POP3Server.Close
}

// MockS3Storage is a mock implementation of the S3StorageInterface
type MockS3Storage struct {
	mock.Mock
}

// Ensure MockS3Storage implements testutils.S3StorageInterface
var _ testutils.S3StorageInterface = (*MockS3Storage)(nil)

func (m *MockS3Storage) Get(key string) (io.ReadCloser, error) {
	args := m.Called(key)
	if args.Get(0) == nil { // Handle nil return for io.ReadCloser
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockS3Storage) Put(key string, body io.Reader, size int64) error {
	args := m.Called(key, body, size)
	return args.Error(0)
}

func (m *MockS3Storage) Delete(key string) error {
	args := m.Called(key)
	return args.Error(0)
}

func (m *MockS3Storage) Exists(key string) (bool, string, error) {
	args := m.Called(key)
	return args.Bool(0), args.String(1), args.Error(2)
}

func (m *MockS3Storage) Copy(sourcePath, destPath string) error {
	args := m.Called(sourcePath, destPath)
	return args.Error(0)
}

// MockUploadWorker is a mock implementation of the UploadWorkerInterface
type MockUploadWorker struct {
	mock.Mock
}

// Ensure MockUploadWorker implements testutils.UploadWorkerInterface
var _ testutils.UploadWorkerInterface = (*MockUploadWorker)(nil)

func (m *MockUploadWorker) GetLocalFile(contentHash string) ([]byte, error) {
	args := m.Called(contentHash)
	if args.Get(0) == nil { // Handle nil return for []byte
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockUploadWorker) StoreLocally(contentHash string, data []byte) (*string, error) {
	args := m.Called(contentHash, data)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*string), args.Error(1)
}

func (m *MockUploadWorker) RemoveLocalFile(path string) error {
	args := m.Called(path)
	return args.Error(0)
}

func (m *MockUploadWorker) FilePath(contentHash string) string {
	args := m.Called(contentHash)
	return args.String(0)
}

func (m *MockUploadWorker) NotifyUploadQueued() {
	m.Called()
}

// MockCache is a mock implementation of the CacheInterface
type MockCache struct {
	mock.Mock
}

// Ensure MockCache implements testutils.CacheInterface
var _ testutils.CacheInterface = (*MockCache)(nil)

func (m *MockCache) Get(contentHash string) ([]byte, error) {
	args := m.Called(contentHash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockCache) Put(contentHash string, data []byte) error {
	args := m.Called(contentHash, data)
	return args.Error(0)
}

func (m *MockCache) Delete(contentHash string) error {
	args := m.Called(contentHash)
	return args.Error(0)
}

// MockConn is a mock implementation of the net.Conn interface
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	// This mock Read is tricky. It needs to be driven by the test.
	// A common pattern is to use a bytes.Buffer or a pipe.
	args := m.Called(b)
	if len(args) > 0 {
		// Make sure we're returning the correct types
		if n, ok := args.Get(0).(int); ok {
			return n, args.Error(1)
		}
	}
	// Default implementation for tests that don't set expectations
	return 0, io.EOF
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	args := m.Called(b) // This mock Write will write to a buffer provided by the test
	if len(args) > 0 {
		// Make sure we're returning the correct types
		if n, ok := args.Get(0).(int); ok {
			return n, args.Error(1)
		}
	}
	// Default implementation for tests that don't set expectations
	return len(b), nil
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConn) LocalAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m *MockConn) RemoteAddr() net.Addr {
	args := m.Called()
	return args.Get(0).(net.Addr)
}

func (m *MockConn) SetDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockConn) SetReadDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockConn) SetWriteDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

// MockAddr is a mock implementation of the net.Addr interface
type MockAddr struct {
	mock.Mock
	// net.Addr // No need to embed
}

func (m *MockAddr) Network() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockAddr) String() string {
	args := m.Called()
	return args.String(0)
}
