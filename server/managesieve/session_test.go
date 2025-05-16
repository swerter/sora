package managesieve

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestManageSieveSessionLoginCommand verifies that the LOGIN command is handled correctly
func TestManageSieveSessionLoginCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Verify that the session has the correct user ID
	assert.Equal(t, int64(123), session.UserID())
	assert.True(t, session.authenticated)

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionListScriptsCommand verifies that the LISTSCRIPTS command is handled correctly
func TestManageSieveSessionListScriptsCommand(t *testing.T) {

	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test scripts
	scripts := []*db.SieveScript{
		{ID: 1, Name: "script1", Active: true},
		{ID: 2, Name: "script2", Active: false},
	}
	mockDB.On("GetUserScripts", mock.Anything, int64(123)).Return(scripts, nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the LISTSCRIPTS command
	_, err = clientConn.Write([]byte("LISTSCRIPTS\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK script1 script2 \r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionGetScriptCommand verifies that the GETSCRIPT command is handled correctly
func TestManageSieveSessionGetScriptCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test script
	script := &db.SieveScript{
		ID:     1,
		Name:   "myscript",
		Script: "require \"fileinto\";\nif header :contains \"Subject\" \"Important\" {\n  fileinto \"INBOX.important\";\n}\n",
		Active: true,
	}
	mockDB.On("GetScriptByName", mock.Anything, "myscript", int64(123)).Return(script, nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the GETSCRIPT command
	_, err = clientConn.Write([]byte("GETSCRIPT myscript\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	expectedResponse := fmt.Sprintf("+OK %d\r\n%s\r\n", len(script.Script), script.Script)
	assert.Equal(t, expectedResponse, string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionPutScriptCommand verifies that the PUTSCRIPT command is handled correctly
func TestManageSieveSessionPutScriptCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Mock script creation - use a simpler script to avoid issues with newlines
	scriptContent := "require \"fileinto\";"
	createdScript := &db.SieveScript{
		ID:     1,
		UserID: 123,
		Name:   "newscript",
		Script: scriptContent,
		Active: false,
	}
	mockDB.On("GetScriptByName", mock.Anything, "newscript", int64(123)).Return(nil, consts.ErrDBNotFound).Once()
	mockDB.On("CreateScript", mock.Anything, int64(123), "newscript", scriptContent).Return(createdScript, nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the PUTSCRIPT command - we need to ensure the exact same content is sent
	// as what we mocked in the CreateScript call
	_, err = clientConn.Write([]byte("PUTSCRIPT newscript require \"fileinto\";\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Script stored\r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionSetActiveCommand verifies that the SETACTIVE command is handled correctly
func TestManageSieveSessionSetActiveCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test script
	script := &db.SieveScript{
		ID:     1,
		Name:   "myscript",
		Script: "require \"fileinto\";\nif header :contains \"Subject\" \"Important\" {\n  fileinto \"INBOX.important\";\n}\n",
		Active: false,
	}
	mockDB.On("GetScriptByName", mock.Anything, "myscript", int64(123)).Return(script, nil).Once()
	mockDB.On("SetScriptActive", mock.Anything, int64(1), int64(123), true).Return(nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the SETACTIVE command
	_, err = clientConn.Write([]byte("SETACTIVE myscript\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Script activated\r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionDeleteScriptCommand verifies that the DELETESCRIPT command is handled correctly
func TestManageSieveSessionDeleteScriptCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Create test script
	script := &db.SieveScript{
		ID:     1,
		Name:   "myscript",
		Script: "require \"fileinto\";\nif header :contains \"Subject\" \"Important\" {\n  fileinto \"INBOX.important\";\n}\n",
		Active: false,
	}
	mockDB.On("GetScriptByName", mock.Anything, "myscript", int64(123)).Return(script, nil).Once()
	mockDB.On("DeleteScript", mock.Anything, int64(1), int64(123)).Return(nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the DELETESCRIPT command
	_, err = clientConn.Write([]byte("DELETESCRIPT myscript\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Script deleted\r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionNoopCommand verifies that the NOOP command is handled correctly
func TestManageSieveSessionNoopCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send the NOOP command
	_, err = clientConn.Write([]byte("NOOP\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK\r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// TestManageSieveSessionLogoutCommand verifies that the LOGOUT command is handled correctly
func TestManageSieveSessionLogoutCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGOUT command
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}

// TestManageSieveSessionUnknownCommand verifies that unknown commands are handled correctly
func TestManageSieveSessionUnknownCommand(t *testing.T) {
	// Create a pipe connection
	clientConn, serverConn := NewPipeConn()

	// Create mock dependencies
	mockDB := new(MockDatabase)
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil).Once()
	mockDB.On("Authenticate", mock.Anything, int64(123), "password123").Return(nil).Once()

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   context.Background(),
	}

	// Create session context
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	// Create the ManageSieveSession with the server connection
	var conn net.Conn = serverConn
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,
		reader: bufio.NewReader(serverConn),
		writer: bufio.NewWriter(serverConn),
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = serverConn.RemoteAddr().String()
	session.Protocol = "ManageSieve"
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
	assert.Equal(t, "+OK ManageSieve ready\r\n", string(greeting[:n]))

	// Send the LOGIN command
	_, err = clientConn.Write([]byte("LOGIN user@example.com password123\r\n"))
	assert.NoError(t, err)

	// Read the response
	response := make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "+OK Authenticated\r\n", string(response[:n]))

	// Send an unknown command
	_, err = clientConn.Write([]byte("UNKNOWN\r\n"))
	assert.NoError(t, err)

	// Read the response
	response = make([]byte, 256)
	n, err = clientConn.Read(response)
	assert.NoError(t, err)
	assert.Equal(t, "-ERR Unknown command\r\n", string(response[:n]))

	// Send the LOGOUT command to end the session
	_, err = clientConn.Write([]byte("LOGOUT\r\n"))
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

// --- Helper functions for setting up mock sessions ---

type mockSessionSetup struct {
	session  *ManageSieveSession
	mockDB   *MockDatabase
	mockConn *MockConn
}

// newMockSession creates a ManageSieveSession with mock dependencies and a mock network connection.
// It returns the session and the mock components for setting expectations and reading/writing.
func newMockSession(_ *testing.T) *mockSessionSetup {
	mockDB := new(MockDatabase)

	// Mock the ManageSieveServer
	mockServer := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
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

	// Create the ManageSieveSession
	session := &ManageSieveSession{
		server: mockServer,
		conn:   &conn,                                  // Pass a pointer to the net.Conn interface
		reader: bufio.NewReader(strings.NewReader("")), // Empty reader
		writer: bufio.NewWriter(serverWriter),          // Writer that writes to our buffer
		ctx:    sessionCtx,
		cancel: sessionCancel,
	}

	// Set basic session info
	session.RemoteIP = "127.0.0.1:12345"
	session.Protocol = "ManageSieve"
	session.Id = "test-session-id"
	session.HostName = mockServer.hostname

	return &mockSessionSetup{
		session:  session,
		mockDB:   mockDB,
		mockConn: mockConn,
	}
}
