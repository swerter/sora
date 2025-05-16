package managesieve

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/migadu/sora/db"
	"github.com/stretchr/testify/mock"
)

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

func (m *MockDatabase) Authenticate(ctx context.Context, userID int64, password string) error {
	args := m.Called(ctx, userID, password)
	return args.Error(0)
}

func (m *MockDatabase) GetUserScripts(ctx context.Context, userID int64) ([]*db.SieveScript, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*db.SieveScript), args.Error(1)
}

func (m *MockDatabase) GetScriptByName(ctx context.Context, name string, userID int64) (*db.SieveScript, error) {
	args := m.Called(ctx, name, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.SieveScript), args.Error(1)
}

func (m *MockDatabase) GetActiveScript(ctx context.Context, userID int64) (*db.SieveScript, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.SieveScript), args.Error(1)
}

func (m *MockDatabase) CreateScript(ctx context.Context, userID int64, name, script string) (*db.SieveScript, error) {
	args := m.Called(ctx, userID, name, script)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.SieveScript), args.Error(1)
}

func (m *MockDatabase) UpdateScript(ctx context.Context, scriptID, userID int64, name, script string) (*db.SieveScript, error) {
	args := m.Called(ctx, scriptID, userID, name, script)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*db.SieveScript), args.Error(1)
}

func (m *MockDatabase) SetScriptActive(ctx context.Context, scriptID, userID int64, active bool) error {
	args := m.Called(ctx, scriptID, userID, active)
	return args.Error(0)
}

func (m *MockDatabase) DeleteScript(ctx context.Context, scriptID, userID int64) error {
	args := m.Called(ctx, scriptID, userID)
	return args.Error(0)
}

func (m *MockDatabase) Close() {
	m.Called()
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
