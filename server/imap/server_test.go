package imap

import (
	"context"
	"testing"

	"github.com/emersion/go-imap/v2"
	"github.com/stretchr/testify/assert"
)

// TestIMAPServerCapabilities verifies that the IMAP server has the expected capabilities
func TestIMAPServerCapabilities(t *testing.T) {
	// Create a new IMAP server with mock dependencies
	ctx := context.Background()
	server, err := New(
		ctx,
		"test.example.com",
		"127.0.0.1:143",
		nil,   // storage
		nil,   // database
		nil,   // uploader
		nil,   // cache
		false, // insecureAuth
		false, // debug
		"",    // TLS certificate file
		"",    // TLS key file
	)

	// Verify that the server was created successfully
	assert.NoError(t, err)
	assert.NotNil(t, server)

	// Verify that the server has the expected capabilities
	assert.Contains(t, server.caps, imap.CapIMAP4rev1)
	assert.Contains(t, server.caps, imap.CapMove)
	assert.Contains(t, server.caps, imap.CapIdle)
}
