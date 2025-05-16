package pop3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPOP3ServerStructure verifies that the POP3Server struct exists
func TestPOP3ServerStructure(t *testing.T) {
	// Create a new POP3Server
	server := &POP3Server{
		hostname: "test.example.com",
		addr:     "127.0.0.1:110",
	}

	// Verify that the server was created successfully
	assert.NotNil(t, server)
	assert.Equal(t, "test.example.com", server.hostname)
	assert.Equal(t, "127.0.0.1:110", server.addr)
}

// TestPOP3SessionStructure verifies that the POP3Session struct exists
func TestPOP3SessionStructure(t *testing.T) {
	// Create a new POP3Session
	session := &POP3Session{
		deleted: make(map[int]bool),
	}
	session.Session.Protocol = "POP3"
	session.Session.Id = "test-session"

	// Verify that the session was created successfully
	assert.NotNil(t, session)
	assert.Equal(t, "POP3", session.Session.Protocol)
	assert.Equal(t, "test-session", session.Session.Id)
	assert.NotNil(t, session.deleted)
}
