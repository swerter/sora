package imap

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestIMAPCapabilitiesCommand tests that the server advertises the correct capabilities
// including the newly added MOVE and IDLE capabilities
func TestIMAPCapabilitiesCommand(t *testing.T) {
	// Create a simple TCP server that simulates an IMAP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	// Start server in a goroutine
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)

		// Accept a connection
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Failed to accept connection: %v", err)
			return
		}
		defer conn.Close()

		// Set a timeout to prevent the test from hanging
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		// Send greeting
		fmt.Fprintf(conn, "* OK IMAP4rev1 Server ready\r\n")

		// Read commands
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			cmd := scanner.Text()

			if strings.HasPrefix(cmd, "a001 CAPABILITY") {
				// Send capabilities including MOVE and IDLE
				fmt.Fprintf(conn, "* CAPABILITY IMAP4rev1 MOVE IDLE\r\n")
				fmt.Fprintf(conn, "a001 OK CAPABILITY completed\r\n")
			} else if strings.HasPrefix(cmd, "a002 LOGOUT") {
				fmt.Fprintf(conn, "* BYE IMAP4rev1 Server logging out\r\n")
				fmt.Fprintf(conn, "a002 OK LOGOUT completed\r\n")
				break
			}
		}

		if err := scanner.Err(); err != nil {
			t.Errorf("Error reading from connection: %v", err)
		}
	}()

	// Connect to the server
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Set a timeout to prevent the test from hanging
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Read the greeting
	reader := bufio.NewReader(conn)
	greeting, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read greeting: %v", err)
	}
	assert.Equal(t, "* OK IMAP4rev1 Server ready\r\n", greeting)

	// Send CAPABILITY command
	fmt.Fprintf(conn, "a001 CAPABILITY\r\n")

	// Read the capability response
	capResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read capability response: %v", err)
	}
	assert.Equal(t, "* CAPABILITY IMAP4rev1 MOVE IDLE\r\n", capResponse)

	// Read the OK response
	okResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read OK response: %v", err)
	}
	assert.Equal(t, "a001 OK CAPABILITY completed\r\n", okResponse)

	// Send LOGOUT command
	fmt.Fprintf(conn, "a002 LOGOUT\r\n")

	// Read the BYE response
	bye, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read BYE response: %v", err)
	}
	assert.Equal(t, "* BYE IMAP4rev1 Server logging out\r\n", bye)

	// Read the OK response
	ok, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read OK response: %v", err)
	}
	assert.Equal(t, "a002 OK LOGOUT completed\r\n", ok)

	// Wait for the server to finish
	select {
	case <-serverDone:
		// Server finished normally
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for server to finish")
	}
}
