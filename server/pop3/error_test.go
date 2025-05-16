package pop3

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPOP3ErrorHandling verifies that client errors are handled correctly
func TestPOP3ErrorHandling(t *testing.T) {
	// Create a new POP3Session
	session := &POP3Session{
		errorsCount: 0,
	}
	session.Session.Protocol = "POP3"
	session.Session.Id = "test-session"

	// Create a mock writer
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	// Test handling a client error
	result := session.handleClientError(writer, "-ERR Test error\r\n")
	writer.Flush()

	// Verify that the error was handled correctly
	assert.False(t, result)
	assert.Equal(t, 1, session.errorsCount)
	assert.Equal(t, "-ERR Test error\r\n", buf.String())

	// Reset the buffer
	buf.Reset()

	// Test handling multiple errors
	session.errorsCount = MAX_ERRORS_ALLOWED
	result = session.handleClientError(writer, "-ERR Another error\r\n")
	writer.Flush()

	// Verify that the connection is closed after too many errors
	assert.True(t, result)
	assert.Equal(t, MAX_ERRORS_ALLOWED+1, session.errorsCount)
	assert.Equal(t, "-ERR Too many errors, closing connection\r\n", buf.String())
}

// TestPOP3SessionReset verifies that the session can be reset
func TestPOP3SessionReset(t *testing.T) {
	// Create a new POP3Session
	session := &POP3Session{
		deleted: make(map[int]bool),
	}
	session.deleted[1] = true
	session.deleted[2] = true

	// Verify initial state
	assert.Equal(t, 2, len(session.deleted))
	assert.True(t, session.deleted[1])
	assert.True(t, session.deleted[2])

	// Reset the session (simulate RSET command)
	session.deleted = make(map[int]bool)

	// Verify that the deleted map was reset
	assert.Equal(t, 0, len(session.deleted))
	assert.False(t, session.deleted[1])
	assert.False(t, session.deleted[2])
}
