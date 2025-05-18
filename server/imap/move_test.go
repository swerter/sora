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

// TestIMAPMoveCommand tests the MOVE command functionality
func TestIMAPMoveCommand(t *testing.T) {
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
			} else if strings.HasPrefix(cmd, "a003 MOVE") {
				// Simulate MOVE command
				// Expected format: "a003 MOVE 1:2 Trash"
				parts := strings.Split(cmd, " ")
				if len(parts) == 4 && parts[2] == "1:2" && parts[3] == "Trash" {
					// Successful MOVE response
					fmt.Fprintf(conn, "* OK [COPYUID 1234567890 1:2 101:102] Messages moved\r\n")
					fmt.Fprintf(conn, "* 2 EXPUNGE\r\n")
					fmt.Fprintf(conn, "* 1 EXPUNGE\r\n")
					fmt.Fprintf(conn, "a003 OK MOVE completed\r\n")
				} else {
					fmt.Fprintf(conn, "a003 NO [TRYCREATE] Destination mailbox does not exist\r\n")
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

	// Send MOVE command
	fmt.Fprintf(conn, "a003 MOVE 1:2 Trash\r\n")

	// Read MOVE responses
	copyuidResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read COPYUID response: %v", err)
	}
	assert.Equal(t, "* OK [COPYUID 1234567890 1:2 101:102] Messages moved\r\n", copyuidResponse)

	expunge1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read first EXPUNGE response: %v", err)
	}
	assert.Equal(t, "* 2 EXPUNGE\r\n", expunge1)

	expunge2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read second EXPUNGE response: %v", err)
	}
	assert.Equal(t, "* 1 EXPUNGE\r\n", expunge2)

	moveOK, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read MOVE OK response: %v", err)
	}
	assert.Equal(t, "a003 OK MOVE completed\r\n", moveOK)

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
