package managesieve

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestManageSieveErrorHandling verifies that errors are handled correctly
func TestManageSieveErrorHandling(t *testing.T) {
	// Create a new ManageSieveSession
	session := &ManageSieveSession{}
	session.Session.Protocol = "ManageSieve"
	session.Session.Id = "test-session"

	// Create a mock writer
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	session.writer = writer

	// Test sending an error response
	session.sendResponse("-ERR Test error\r\n")
	writer.Flush()

	// Verify that the error was sent correctly
	assert.Equal(t, "-ERR Test error\r\n", buf.String())

	// Reset the buffer
	buf.Reset()

	// Test sending a success response
	session.sendResponse("+OK Success\r\n")
	writer.Flush()

	// Verify that the success response was sent correctly
	assert.Equal(t, "+OK Success\r\n", buf.String())
}
