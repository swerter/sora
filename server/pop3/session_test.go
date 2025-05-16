package pop3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestPOP3SessionUserCommand verifies that the USER command is handled correctly
func TestPOP3SessionUserCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Verify that the session has the correct user ID and mailbox ID
	assert.Equal(t, int64(123), session.UserID())
	assert.Equal(t, int64(456), session.inboxMailboxID)

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}

// TestPOP3SessionPassCommand verifies that the PASS command is handled correctly
func TestPOP3SessionPassCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command first
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Send the PASS command
	_, err = clientConn.Write([]byte("PASS password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Password accepted\r\n", string(response[:n]))

	// Verify that the session is authenticated
	assert.True(t, session.authenticated)

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}

// TestPOP3SessionStatCommand verifies that the STAT command is handled correctly
func TestPOP3SessionStatCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()
	mockDB.On("GetMailboxMessageCountAndSizeSum", mock.Anything, int64(456)).Return(5, int64(12345), nil).Once()
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Send the PASS command
	_, err = clientConn.Write([]byte("PASS password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Password accepted\r\n", string(response[:n]))

	// Send the STAT command
	_, err = clientConn.Write([]byte("STAT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK 5 12345\r\n", string(response[:n]))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}

// TestPOP3SessionListCommand verifies that the LIST command is handled correctly
func TestPOP3SessionListCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test messages
	messages := []db.Message{
		{UID: 1, Size: 1000},
		{UID: 2, Size: 2000},
		{UID: 3, Size: 3000},
	}
	mockDB.On("ListMessages", mock.Anything, int64(456)).Return(messages, nil).Once()
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Send the PASS command
	_, err = clientConn.Write([]byte("PASS password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Password accepted\r\n", string(response[:n]))

	// Send the LIST command
	_, err = clientConn.Write([]byte("LIST\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 512)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)

	// Check the response
	expectedResponse := "+OK scan listing follows\r\n1 1000\r\n2 2000\r\n3 3000\r\n.\r\n"
	assert.Equal(t, expectedResponse, string(response[:n]))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}

// TestPOP3SessionRetrCommand verifies that the RETR command is handled correctly
func TestPOP3SessionRetrCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test messages
	messages := []db.Message{
		{UID: 1, Size: 100, ContentHash: "hash1", IsUploaded: true},
		{UID: 2, Size: 200, ContentHash: "hash2", IsUploaded: true},
	}
	mockDB.On("ListMessages", mock.Anything, int64(456)).Return(messages, nil).Once()
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Set up cache hit for message 1
	messageBody := []byte("From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Test\r\n\r\nThis is a test message.")
	mockCache.On("Get", "hash1").Return(messageBody, nil).Once()

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Send the PASS command
	_, err = clientConn.Write([]byte("PASS password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Password accepted\r\n", string(response[:n]))

	// Send the LIST command to populate the messages
	_, err = clientConn.Write([]byte("LIST\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 512)
	_, err = clientConn.Read(response)
	assert.NoError(t, err)

	// Send the RETR command for message 1
	_, err = clientConn.Write([]byte("RETR 1\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 1024)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)

	// Check the response - we don't check the exact size since it might vary
	// Just verify the response starts with "+OK" and contains the message body
	responseStr := string(response[:n])
	assert.True(t, strings.HasPrefix(responseStr, "+OK "))
	assert.Contains(t, responseStr, string(messageBody))
	assert.True(t, strings.HasSuffix(responseStr, "\r\n.\r\n"))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// TestPOP3SessionDeleCommand verifies that the DELE command is handled correctly
func TestPOP3SessionDeleCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("GetMailboxByName", mock.Anything, int64(123), "INBOX").Return(&db.DBMailbox{ID: 456}, nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Create test messages
	messages := []db.Message{
		{UID: 1, Size: 100, ContentHash: "hash1"},
		{UID: 2, Size: 200, ContentHash: "hash2"},
		{UID: 3, Size: 300, ContentHash: "hash3"},
	}
	mockDB.On("ListMessages", mock.Anything, int64(456)).Return(messages, nil).Once()

	// Mock cache delete for message 2 (which will be deleted)
	mockCache.On("Delete", "hash2").Return(nil).Once()

	// Mock expunge for message 2 (UID 2)
	mockDB.On("ExpungeMessageUIDs", mock.Anything, int64(456), mock.Anything).Return(nil).Once()

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the USER command
	_, err = clientConn.Write([]byte("USER user@example.com\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK User accepted\r\n", string(response[:n]))

	// Send the PASS command
	_, err = clientConn.Write([]byte("PASS password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Password accepted\r\n", string(response[:n]))

	// Send the LIST command to populate the messages
	_, err = clientConn.Write([]byte("LIST\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 512)
	_, err = clientConn.Read(response)
	assert.NoError(t, err)

	// Send the DELE command for message 2
	_, err = clientConn.Write([]byte("DELE 2\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Message deleted\r\n", string(response[:n]))

	// Verify that the message is marked for deletion
	assert.True(t, session.deleted[1]) // 0-based index for message 2

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
	mockCache.AssertExpectations(t)
}

// TestPOP3SessionQuitCommand verifies that the QUIT command is handled correctly
func TestPOP3SessionQuitCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the QUIT command
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)
}

// TestPOP3SessionNoopCommand verifies that the NOOP command is handled correctly
func TestPOP3SessionNoopCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the NOOP command
	_, err = clientConn.Write([]byte("NOOP\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", string(response[:n]))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)
}

// TestPOP3SessionRsetCommand verifies that the RSET command is handled correctly
func TestPOP3SessionRsetCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Add some deleted messages
	session.deleted[1] = true
	session.deleted[2] = true

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send the RSET command
	_, err = clientConn.Write([]byte("RSET\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", string(response[:n]))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)
}

// TestPOP3SessionUnknownCommand verifies that unknown commands are handled correctly
func TestPOP3SessionUnknownCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("ExpungeMessageUIDs", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the POP3Session with the server connection
	var conn net.Conn = serverConn
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn,
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	// Start the session handler in a goroutine
	done := make(chan struct{})
	go func() {
		session.handleConnection()
		close(done)
	}()

	// Read the greeting
	greeting := make([]byte, 256)
	n, err := clientConn.Read(greeting)
	assert.NoError(t, err)
	assert.Equal(t, "+OK POP3 server ready\r\n", string(greeting[:n]))

	// Send an unknown command
	_, err = clientConn.Write([]byte("UNKNOWN\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "-ERR Unknown command: UNKNOWN\r\n", string(response[:n]))

	// Send the QUIT command to end the session
	_, err = clientConn.Write([]byte("QUIT\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Goodbye\r\n", string(response[:n]))

	// Wait for the handler to exit
	<-done

	// Verify the connection was closed
	_, err = clientConn.Write([]byte("TEST\r\n"))
	assert.Error(t, err)
}

// TestPOP3SessionGetMessageBody verifies that message bodies can be retrieved
func TestPOP3SessionGetMessageBody(t *testing.T) {
	// Create a mock session setup
	setup := newMockSession(t)

	// Create a test message
	contentHash := "abc123"
	messageBody := []byte("From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Test\r\n\r\nThis is a test message.")
	message := db.Message{
		UID:         1,
		Size:        len(messageBody),
		ContentHash: contentHash,
		IsUploaded:  true,
	}

	// Test case 1: Cache hit
	setup.mockCache.On("Get", contentHash).Return(messageBody, nil).Once()

	body, err := setup.session.getMessageBody(&message)
	assert.NoError(t, err)
	assert.Equal(t, messageBody, body)

	// Test case 2: Cache miss, S3 hit
	mockReader := io.NopCloser(bytes.NewReader(messageBody))
	setup.mockCache.On("Get", contentHash).Return(nil, fmt.Errorf("cache miss")).Once()
	setup.mockS3.On("Get", contentHash).Return(mockReader, nil).Once()
	setup.mockCache.On("Put", contentHash, messageBody).Return(nil).Once()

	body, err = setup.session.getMessageBody(&message)
	assert.NoError(t, err)
	assert.Equal(t, messageBody, body)

	// Test case 3: Not uploaded, local file exists
	message.IsUploaded = false
	setup.mockUploader.On("GetLocalFile", contentHash).Return(messageBody, nil).Once()

	body, err = setup.session.getMessageBody(&message)
	assert.NoError(t, err)
	assert.Equal(t, messageBody, body)

	// Test case 4: Not uploaded, local file does not exist
	setup.mockUploader.On("GetLocalFile", contentHash).Return(nil, os.ErrNotExist).Once()

	body, err = setup.session.getMessageBody(&message)
	assert.Error(t, err)
	assert.Equal(t, consts.ErrMessageNotAvailable, err)
	assert.Nil(t, body)

	// Verify all expectations were met
	setup.mockCache.AssertExpectations(t)
	setup.mockS3.AssertExpectations(t)
	setup.mockUploader.AssertExpectations(t)
}

// --- Helper functions for setting up mock sessions ---

type mockSessionSetup struct {
	session      *POP3Session
	mockDB       *MockDatabase
	mockS3       *MockS3Storage
	mockUploader *MockUploadWorker
	mockCache    *MockCache
	mockConn     *MockConn
	clientReader *bytes.Buffer // Buffer to write client commands into
	serverWriter *bytes.Buffer // Buffer to read server responses from
}

// newMockSession creates a POP3Session with mock dependencies and a mock network connection.
// It returns the session and the mock components for setting expectations and reading/writing.
func newMockSession(_ *testing.T) *mockSessionSetup {
	mockDB := new(MockDatabase)
	mockS3 := new(MockS3Storage)
	mockUploader := new(MockUploadWorker)
	mockCache := new(MockCache)

	// Mock the POP3Server
	mockServer := &POP3Server{
		hostname: "test.example.com",
		db:       mockDB,
		s3:       mockS3,
		uploader: mockUploader,
		cache:    mockCache,
		appCtx:   context.Background(), // Use a background context for the server
	}

	// Create buffers for reading and writing
	clientReader := new(bytes.Buffer) // Client writes commands here
	serverWriter := new(bytes.Buffer) // Server writes responses here

	// Create a mock connection that uses our buffers
	mockConn := new(MockConn)

	// Set up the Read method to read from clientReader
	mockConn.On("Read", mock.Anything).Run(func(args mock.Arguments) {
		b := args.Get(0).([]byte)
		n, _ := clientReader.Read(b)
		if n == 0 {
			// If there's nothing to read, block until there is
			time.Sleep(10 * time.Millisecond)
		}
	}).Return(func(b []byte) int {
		n, _ := clientReader.Read(b)
		return n
	}, nil).Maybe()

	// Set up the Write method to write to serverWriter
	mockConn.On("Write", mock.Anything).Run(func(args mock.Arguments) {
		b := args.Get(0).([]byte)
		serverWriter.Write(b)
	}).Return(func(b []byte) int {
		return len(b)
	}, nil).Maybe()

	// Set up other methods
	mockConn.On("Close").Return(nil).Maybe()
	mockAddr := new(MockAddr)
	mockAddr.On("String").Return("127.0.0.1:12345").Maybe()
	mockAddr.On("Network").Return("tcp").Maybe()
	mockConn.On("RemoteAddr").Return(mockAddr).Maybe()
	mockConn.On("LocalAddr").Return(mockAddr).Maybe()
	mockConn.On("SetReadDeadline", mock.AnythingOfType("time.Time")).Return(nil).Maybe()
	mockConn.On("SetWriteDeadline", mock.AnythingOfType("time.Time")).Return(nil).Maybe()
	mockConn.On("SetDeadline", mock.AnythingOfType("time.Time")).Return(nil).Maybe()

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create a net.Conn from our mock
	var conn net.Conn = mockConn

	// Create the POP3Session
	session := &POP3Session{
		server:  mockServer,
		conn:    &conn, // Pass a pointer to the net.Conn interface
		deleted: make(map[int]bool),
		ctx:     sessionCtx,
		cancel:  sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = "127.0.0.1:12345"
	session.Protocol = "POP3"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	return &mockSessionSetup{
		session:      session,
		mockDB:       mockDB,
		mockS3:       mockS3,
		mockUploader: mockUploader,
		mockCache:    mockCache,
		mockConn:     mockConn,
		clientReader: clientReader,
		serverWriter: serverWriter,
	}
}
