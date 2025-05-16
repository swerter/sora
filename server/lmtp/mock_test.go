package lmtp

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/sieveengine"
	"github.com/migadu/sora/server/testutils"
	"github.com/stretchr/testify/mock"
)

// Import the interfaces from testutils
type S3StorageInterface = testutils.S3StorageInterface
type UploadWorkerInterface = testutils.UploadWorkerInterface

// DBer is an interface for database operations specific to LMTP
type DBer interface {
	GetUserIDByAddress(ctx context.Context, address string) (int64, error)
	GetMailboxByName(ctx context.Context, userID int64, name string) (*db.DBMailbox, error)
	InsertMessage(ctx context.Context, options *db.InsertMessageOptions, upload db.PendingUpload) (int64, int64, error)
	Close()
}

// SieveExecutorInterface defines the interface for sieve script execution
type SieveExecutorInterface interface {
	Evaluate(ctx sieveengine.Context) (sieveengine.Result, error)
}

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

func (m *MockDatabase) InsertMessage(ctx context.Context, options *db.InsertMessageOptions, upload db.PendingUpload) (int64, int64, error) {
	args := m.Called(ctx, options, upload)
	return args.Get(0).(int64), args.Get(1).(int64), args.Error(2)
}

func (m *MockDatabase) Close() {
	m.Called()
}

// MockS3Storage is a mock implementation of the S3StorageInterface
type MockS3Storage struct {
	mock.Mock
}

// Ensure MockS3Storage implements S3StorageInterface
var _ S3StorageInterface = (*MockS3Storage)(nil)

func (m *MockS3Storage) Get(key string) (io.ReadCloser, error) {
	args := m.Called(key)
	if args.Get(0) == nil {
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

// Ensure MockUploadWorker implements UploadWorkerInterface
var _ UploadWorkerInterface = (*MockUploadWorker)(nil)

func (m *MockUploadWorker) GetLocalFile(contentHash string) ([]byte, error) {
	args := m.Called(contentHash)
	if args.Get(0) == nil {
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

// MockSieveExecutor is a mock implementation of the SieveExecutorInterface
type MockSieveExecutor struct {
	mock.Mock
}

// Ensure MockSieveExecutor implements SieveExecutorInterface
var _ SieveExecutorInterface = (*MockSieveExecutor)(nil)

func (m *MockSieveExecutor) Evaluate(ctx sieveengine.Context) (sieveengine.Result, error) {
	args := m.Called(ctx)
	return args.Get(0).(sieveengine.Result), args.Error(1)
}

// PipeConn is a simple implementation of net.Conn using io.Pipe
type PipeConn struct {
	reader       *io.PipeReader
	writer       *io.PipeWriter
	localAddr    net.Addr
	remoteAddr   net.Addr
	readDeadline time.Time
}

// NewPipeConn creates a new PipeConn
func NewPipeConn() (*PipeConn, *PipeConn) {
	// Create two pipes, one for each direction
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	addr1 := &mockAddr{network: "pipe", address: "addr1"}
	addr2 := &mockAddr{network: "pipe", address: "addr2"}

	// Create two connections, each with its own reader and writer
	conn1 := &PipeConn{
		reader:     r1,
		writer:     w2,
		localAddr:  addr1,
		remoteAddr: addr2,
	}

	conn2 := &PipeConn{
		reader:     r2,
		writer:     w1,
		localAddr:  addr2,
		remoteAddr: addr1,
	}

	return conn1, conn2
}

// Read reads data from the connection
func (c *PipeConn) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

// Write writes data to the connection
func (c *PipeConn) Write(b []byte) (n int, err error) {
	return c.writer.Write(b)
}

// Close closes the connection
func (c *PipeConn) Close() error {
	c.reader.Close()
	return c.writer.Close()
}

// LocalAddr returns the local network address
func (c *PipeConn) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr returns the remote network address
func (c *PipeConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// SetDeadline sets the read and write deadlines
func (c *PipeConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

// SetReadDeadline sets the read deadline
func (c *PipeConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

// SetWriteDeadline sets the write deadline
func (c *PipeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// mockAddr implements net.Addr for testing
type mockAddr struct {
	network string
	address string
}

func (a *mockAddr) Network() string {
	return a.network
}

func (a *mockAddr) String() string {
	return a.address
}

// MockConn is a mock implementation of the net.Conn interface
type MockConn struct {
	mock.Mock
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	args := m.Called(b)
	if len(args) > 0 {
		if n, ok := args.Get(0).(int); ok {
			return n, args.Error(1)
		}
	}
	return 0, io.EOF
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	args := m.Called(b)
	if len(args) > 0 {
		if n, ok := args.Get(0).(int); ok {
			return n, args.Error(1)
		}
	}
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
}

func (m *MockAddr) Network() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockAddr) String() string {
	args := m.Called()
	return args.String(0)
}
