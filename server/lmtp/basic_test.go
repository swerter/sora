package lmtp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLMTPServerBackendStructure verifies that the LMTPServerBackend struct exists
func TestLMTPServerBackendStructure(t *testing.T) {
	// Create a new LMTPServerBackend
	backend := &LMTPServerBackend{
		hostname: "test.example.com",
		addr:     "127.0.0.1:2525",
	}

	// Verify that the backend was created successfully
	assert.NotNil(t, backend)
	assert.Equal(t, "test.example.com", backend.hostname)
	assert.Equal(t, "127.0.0.1:2525", backend.addr)
}

// TestLMTPSessionStructure verifies that the LMTPSession struct exists
func TestLMTPSessionStructure(t *testing.T) {
	// Create a new LMTPSession
	session := &LMTPSession{}
	session.Session.Protocol = "LMTP"
	session.Session.Id = "test-session"

	// Verify that the session was created successfully
	assert.NotNil(t, session)
	assert.Equal(t, "LMTP", session.Session.Protocol)
	assert.Equal(t, "test-session", session.Session.Id)
}
