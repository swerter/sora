package managesieve

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestManageSieveServerStructure verifies that the ManageSieveServer struct exists
func TestManageSieveServerStructure(t *testing.T) {
	// Create a new ManageSieveServer
	server := &ManageSieveServer{
		hostname: "test.example.com",
		addr:     "127.0.0.1:4190",
	}

	// Verify that the server was created successfully
	assert.NotNil(t, server)
	assert.Equal(t, "test.example.com", server.hostname)
	assert.Equal(t, "127.0.0.1:4190", server.addr)
}

// TestManageSieveSessionStructure verifies that the ManageSieveSession struct exists
func TestManageSieveSessionStructure(t *testing.T) {
	// Create a new ManageSieveSession
	session := &ManageSieveSession{}
	session.Session.Protocol = "ManageSieve"
	session.Session.Id = "test-session"

	// Verify that the session was created successfully
	assert.NotNil(t, session)
	assert.Equal(t, "ManageSieve", session.Session.Protocol)
	assert.Equal(t, "test-session", session.Session.Id)
}
