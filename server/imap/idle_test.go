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

// TestIMAPIdleCommand tests the IDLE command functionality
func TestIMAPIdleCommand(t *testing.T) {
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

			if strings.HasPrefix(cmd, "a001 LOGIN") {
				// Check if the login credentials are correct
				if cmd == "a001 LOGIN user@domain.com password" {
					fmt.Fprintf(conn, "a001 OK LOGIN completed\r\n")
				} else {
					fmt.Fprintf(conn, "a001 NO [AUTHENTICATIONFAILED] Invalid credentials\r\n")
				}
			} else if strings.HasPrefix(cmd, "a002 SELECT") {
				// Simulate selecting INBOX
				fmt.Fprintf(conn, "* 3 EXISTS\r\n")
				fmt.Fprintf(conn, "* 0 RECENT\r\n")
				fmt.Fprintf(conn, "* OK [UIDVALIDITY 1234567890] UIDs valid\r\n")
				fmt.Fprintf(conn, "* OK [UIDNEXT 4] Predicted next UID\r\n")
				fmt.Fprintf(conn, "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n")
				fmt.Fprintf(conn, "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)] Permanent flags\r\n")
				fmt.Fprintf(conn, "a002 OK [READ-WRITE] SELECT completed\r\n")
			} else if strings.HasPrefix(cmd, "a003 IDLE") {
				// Simulate IDLE command
				fmt.Fprintf(conn, "+ idling\r\n")

				// Wait for DONE command
				if scanner.Scan() {
					doneCmd := scanner.Text()
					if doneCmd == "DONE" {
						// Simulate a notification during IDLE
						fmt.Fprintf(conn, "* 1 EXISTS\r\n")
						fmt.Fprintf(conn, "a003 OK IDLE completed\r\n")
					} else {
						fmt.Fprintf(conn, "a003 BAD Expected DONE\r\n")
					}
				}
			} else if strings.HasPrefix(cmd, "a004 LOGOUT") {
				fmt.Fprintf(conn, "* BYE IMAP4rev1 Server logging out\r\n")
				fmt.Fprintf(conn, "a004 OK LOGOUT completed\r\n")
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

	// Send LOGIN command
	fmt.Fprintf(conn, "a001 LOGIN user@domain.com password\r\n")

	// Read the login response
	loginResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read login response: %v", err)
	}
	assert.Equal(t, "a001 OK LOGIN completed\r\n", loginResponse)

	// Send SELECT command
	fmt.Fprintf(conn, "a002 SELECT INBOX\r\n")

	// Read SELECT responses (multiple lines)
	for i := 0; i < 7; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read SELECT response line %d: %v", i+1, err)
		}
	}

	// Send IDLE command
	fmt.Fprintf(conn, "a003 IDLE\r\n")

	// Read the continuation response
	idleResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read IDLE continuation response: %v", err)
	}
	assert.Equal(t, "+ idling\r\n", idleResponse)

	// Wait a bit to simulate idle time
	time.Sleep(100 * time.Millisecond)

	// Send DONE to end the IDLE command
	fmt.Fprintf(conn, "DONE\r\n")

	// Read the EXISTS notification that occurred during IDLE
	existsNotification, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read EXISTS notification: %v", err)
	}
	assert.Equal(t, "* 1 EXISTS\r\n", existsNotification)

	// Read the IDLE completion response
	idleCompletionResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read IDLE completion response: %v", err)
	}
	assert.Equal(t, "a003 OK IDLE completed\r\n", idleCompletionResponse)

	// Send LOGOUT command
	fmt.Fprintf(conn, "a004 LOGOUT\r\n")

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
	assert.Equal(t, "a004 OK LOGOUT completed\r\n", ok)

	// Wait for the server to finish
	select {
	case <-serverDone:
		// Server finished normally
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for server to finish")
	}
}
