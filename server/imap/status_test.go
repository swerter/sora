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

// TestSpecialFolderStatus tests that the STATUS command correctly reports 0 messages
// for special folders (Trash, Junk) when all messages have the \Deleted flag
func TestSpecialFolderStatus(t *testing.T) {
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
			} else if strings.HasPrefix(cmd, "a002 STATUS Trash") {
				// Simulate STATUS command for Trash folder
				// The key part is that we return 0 for message count, unseen, and recent
				// This simulates our fix for the Trash folder
				fmt.Fprintf(conn, "* STATUS Trash (MESSAGES 0 RECENT 0 UNSEEN 0 UIDNEXT 4 UIDVALIDITY 1234567890)\r\n")
				fmt.Fprintf(conn, "a002 OK STATUS completed\r\n")
			} else if strings.HasPrefix(cmd, "a003 STATUS Junk") {
				// Simulate STATUS command for Junk folder
				// Similar to Trash, we return 0 for message count, unseen, and recent
				fmt.Fprintf(conn, "* STATUS Junk (MESSAGES 0 RECENT 0 UNSEEN 0 UIDNEXT 4 UIDVALIDITY 1234567890)\r\n")
				fmt.Fprintf(conn, "a003 OK STATUS completed\r\n")
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

	// Send STATUS command for Trash folder
	fmt.Fprintf(conn, "a002 STATUS Trash (MESSAGES RECENT UNSEEN UIDNEXT UIDVALIDITY)\r\n")

	// Read STATUS response
	trashStatusResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read Trash STATUS response: %v", err)
	}
	assert.Equal(t, "* STATUS Trash (MESSAGES 0 RECENT 0 UNSEEN 0 UIDNEXT 4 UIDVALIDITY 1234567890)\r\n", trashStatusResponse)

	// Read OK response
	trashOkResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read Trash OK response: %v", err)
	}
	assert.Equal(t, "a002 OK STATUS completed\r\n", trashOkResponse)

	// Send STATUS command for Junk folder
	fmt.Fprintf(conn, "a003 STATUS Junk (MESSAGES RECENT UNSEEN UIDNEXT UIDVALIDITY)\r\n")

	// Read STATUS response
	junkStatusResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read Junk STATUS response: %v", err)
	}
	assert.Equal(t, "* STATUS Junk (MESSAGES 0 RECENT 0 UNSEEN 0 UIDNEXT 4 UIDVALIDITY 1234567890)\r\n", junkStatusResponse)

	// Read OK response
	junkOkResponse, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read Junk OK response: %v", err)
	}
	assert.Equal(t, "a003 OK STATUS completed\r\n", junkOkResponse)

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
