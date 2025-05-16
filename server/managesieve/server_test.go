package managesieve

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewManageSieveServer tests the creation of a new ManageSieve server
func TestNewManageSieveServer(t *testing.T) {
	// Create a background context
	ctx := context.Background()

	// Create a new ManageSieve server with nil dependencies
	// In a real test, we would use proper mocks
	server, err := New(
		ctx,
		"test.example.com",
		"127.0.0.1:4190",
		nil, // db
		false,
		false,
	)

	// Verify that the server was created successfully
	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, "test.example.com", server.hostname)
	assert.Equal(t, "127.0.0.1:4190", server.addr)
}

// TestManageSieveServerClose tests closing the ManageSieve server
func TestManageSieveServerClose(t *testing.T) {
	// Create a background context
	ctx := context.Background()

	// Create a new ManageSieve server with nil dependencies
	server, err := New(
		ctx,
		"test.example.com",
		"127.0.0.1:4190",
		nil, // db
		false,
		false,
	)
	assert.NoError(t, err)
	assert.NotNil(t, server)

	// Close the server
	server.Close()
	// No assertions needed, just make sure it doesn't panic
}

// TestManageSieveServerStart tests starting the ManageSieve server
func TestManageSieveServerStart(t *testing.T) {
	t.Skip("Skipping test that requires network access")
}
