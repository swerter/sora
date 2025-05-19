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

// TestExpungeMovesToTrash tests that messages marked as \Deleted in non-Trash folders
// are moved to the Trash folder when expunged, rather than being permanently deleted.
func TestExpungeMovesToTrash(t *testing.T) {
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
			} else if strings.HasPrefix(cmd, "a003 STORE") {
				// Simulate STORE command to mark a message as deleted
				// Expected format: "a003 STORE 1 +FLAGS (\\Deleted)"
				parts := strings.Split(cmd, " ")
				if len(parts) >= 5 && parts[2] == "1" && parts[3] == "+FLAGS" && strings.Contains(parts[4], "\\Deleted") {
					fmt.Fprintf(conn, "* 1 FETCH (FLAGS (\\Deleted))\r\n")
					fmt.Fprintf(conn, "a003 OK STORE completed\r\n")
				} else {
					fmt.Fprintf(conn, "a003 NO Invalid STORE command\r\n")
				}
			} else if strings.HasPrefix(cmd, "a004 EXPUNGE") {
				// Simulate EXPUNGE command - this should move the message to Trash
				fmt.Fprintf(conn, "* 1 EXPUNGE\r\n")
				fmt.Fprintf(conn, "a004 OK EXPUNGE completed\r\n")
			} else if strings.HasPrefix(cmd, "a005 SELECT") {
				// Simulate selecting Trash folder
				if strings.Contains(cmd, "Trash") {
					fmt.Fprintf(conn, "* 1 EXISTS\r\n") // One message should be in Trash now
					fmt.Fprintf(conn, "* 0 RECENT\r\n")
					fmt.Fprintf(conn, "* OK [UIDVALIDITY 1234567891] UIDs valid\r\n")
					fmt.Fprintf(conn, "* OK [UIDNEXT 2] Predicted next UID\r\n")
					fmt.Fprintf(conn, "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n")
					fmt.Fprintf(conn, "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)] Permanent flags\r\n")
					fmt.Fprintf(conn, "a005 OK [READ-WRITE] SELECT completed\r\n")
				} else {
					fmt.Fprintf(conn, "a005 NO [NONEXISTENT] Unknown mailbox\r\n")
				}
			} else if strings.HasPrefix(cmd, "a006 FETCH") {
				// Simulate FETCH command to verify the message is in Trash
				// Expected format: "a006 FETCH 1 (FLAGS)"
				parts := strings.Split(cmd, " ")
				if len(parts) >= 4 && parts[2] == "1" && strings.Contains(parts[3], "FLAGS") {
					// The message in Trash should have both \Seen and \Deleted flags
					fmt.Fprintf(conn, "* 1 FETCH (FLAGS (\\Seen \\Deleted))\r\n")
					fmt.Fprintf(conn, "a006 OK FETCH completed\r\n")
				} else {
					fmt.Fprintf(conn, "a006 NO Invalid FETCH command\r\n")
				}
			} else if strings.HasPrefix(cmd, "a007 LOGOUT") {
				fmt.Fprintf(conn, "* BYE IMAP4rev1 Server logging out\r\n")
				fmt.Fprintf(conn, "a007 OK LOGOUT completed\r\n")
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

	// Send SELECT command for INBOX
	fmt.Fprintf(conn, "a002 SELECT INBOX\r\n")

	// Read SELECT responses (multiple lines)
	for i := 0; i < 7; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read SELECT response line %d: %v", i+1, err)
		}
	}

	// Send STORE command to mark a message as deleted
	fmt.Fprintf(conn, "a003 STORE 1 +FLAGS (\\Deleted)\r\n")

	// Read STORE responses
	storeResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STORE response 1: %v", err)
	}
	assert.Equal(t, "* 1 FETCH (FLAGS (\\Deleted))\r\n", storeResponse1)

	storeResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STORE response 2: %v", err)
	}
	assert.Equal(t, "a003 OK STORE completed\r\n", storeResponse2)

	// Send EXPUNGE command
	fmt.Fprintf(conn, "a004 EXPUNGE\r\n")

	// Read EXPUNGE responses
	expungeResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read EXPUNGE response 1: %v", err)
	}
	assert.Equal(t, "* 1 EXPUNGE\r\n", expungeResponse1)

	expungeResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read EXPUNGE response 2: %v", err)
	}
	assert.Equal(t, "a004 OK EXPUNGE completed\r\n", expungeResponse2)

	// Send SELECT command for Trash folder to verify the message was moved
	fmt.Fprintf(conn, "a005 SELECT Trash\r\n")

	// Read SELECT responses (multiple lines)
	for i := 0; i < 7; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read Trash SELECT response line %d: %v", i+1, err)
		}
	}

	// Send FETCH command to verify the message in Trash has the right flags
	fmt.Fprintf(conn, "a006 FETCH 1 (FLAGS)\r\n")

	// Read FETCH responses
	fetchResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read FETCH response 1: %v", err)
	}
	assert.Equal(t, "* 1 FETCH (FLAGS (\\Seen \\Deleted))\r\n", fetchResponse1)

	fetchResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read FETCH response 2: %v", err)
	}
	assert.Equal(t, "a006 OK FETCH completed\r\n", fetchResponse2)

	// Send LOGOUT command
	fmt.Fprintf(conn, "a007 LOGOUT\r\n")

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
	assert.Equal(t, "a007 OK LOGOUT completed\r\n", ok)

	// Wait for the server to finish
	select {
	case <-serverDone:
		// Server finished normally
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for server to finish")
	}
}

// TestExpungeInTrashPermanentlyDeletes tests that messages marked as \Deleted in the Trash folder
// are permanently deleted when expunged, not moved to another folder.
func TestExpungeInTrashPermanentlyDeletes(t *testing.T) {
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
				// Simulate selecting Trash folder
				if strings.Contains(cmd, "Trash") {
					fmt.Fprintf(conn, "* 1 EXISTS\r\n")
					fmt.Fprintf(conn, "* 0 RECENT\r\n")
					fmt.Fprintf(conn, "* OK [UIDVALIDITY 1234567891] UIDs valid\r\n")
					fmt.Fprintf(conn, "* OK [UIDNEXT 2] Predicted next UID\r\n")
					fmt.Fprintf(conn, "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n")
					fmt.Fprintf(conn, "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)] Permanent flags\r\n")
					fmt.Fprintf(conn, "a002 OK [READ-WRITE] SELECT completed\r\n")
				} else {
					fmt.Fprintf(conn, "a002 NO [NONEXISTENT] Unknown mailbox\r\n")
				}
			} else if strings.HasPrefix(cmd, "a003 STORE") {
				// Simulate STORE command to mark a message as deleted
				// Expected format: "a003 STORE 1 +FLAGS (\\Deleted)"
				parts := strings.Split(cmd, " ")
				if len(parts) >= 5 && parts[2] == "1" && parts[3] == "+FLAGS" && strings.Contains(parts[4], "\\Deleted") {
					fmt.Fprintf(conn, "* 1 FETCH (FLAGS (\\Seen \\Deleted))\r\n")
					fmt.Fprintf(conn, "a003 OK STORE completed\r\n")
				} else {
					fmt.Fprintf(conn, "a003 NO Invalid STORE command\r\n")
				}
			} else if strings.HasPrefix(cmd, "a004 EXPUNGE") {
				// Simulate EXPUNGE command - this should permanently delete the message
				fmt.Fprintf(conn, "* 1 EXPUNGE\r\n")
				fmt.Fprintf(conn, "a004 OK EXPUNGE completed\r\n")
			} else if strings.HasPrefix(cmd, "a005 STATUS") {
				// Simulate STATUS command to verify the message count
				// Expected format: "a005 STATUS Trash (MESSAGES)"
				parts := strings.Split(cmd, " ")
				if len(parts) >= 4 && parts[2] == "Trash" && strings.Contains(parts[3], "MESSAGES") {
					// After expunge, there should be 0 messages in Trash
					fmt.Fprintf(conn, "* STATUS Trash (MESSAGES 0)\r\n")
					fmt.Fprintf(conn, "a005 OK STATUS completed\r\n")
				} else {
					fmt.Fprintf(conn, "a005 NO Invalid STATUS command\r\n")
				}
			} else if strings.HasPrefix(cmd, "a006 LOGOUT") {
				fmt.Fprintf(conn, "* BYE IMAP4rev1 Server logging out\r\n")
				fmt.Fprintf(conn, "a006 OK LOGOUT completed\r\n")
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

	// Send SELECT command for Trash
	fmt.Fprintf(conn, "a002 SELECT Trash\r\n")

	// Read SELECT responses (multiple lines)
	for i := 0; i < 7; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read SELECT response line %d: %v", i+1, err)
		}
	}

	// Send STORE command to mark a message as deleted
	fmt.Fprintf(conn, "a003 STORE 1 +FLAGS (\\Deleted)\r\n")

	// Read STORE responses
	storeResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STORE response 1: %v", err)
	}
	assert.Equal(t, "* 1 FETCH (FLAGS (\\Seen \\Deleted))\r\n", storeResponse1)

	storeResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STORE response 2: %v", err)
	}
	assert.Equal(t, "a003 OK STORE completed\r\n", storeResponse2)

	// Send EXPUNGE command
	fmt.Fprintf(conn, "a004 EXPUNGE\r\n")

	// Read EXPUNGE responses
	expungeResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read EXPUNGE response 1: %v", err)
	}
	assert.Equal(t, "* 1 EXPUNGE\r\n", expungeResponse1)

	expungeResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read EXPUNGE response 2: %v", err)
	}
	assert.Equal(t, "a004 OK EXPUNGE completed\r\n", expungeResponse2)

	// Send STATUS command to verify the message was permanently deleted
	fmt.Fprintf(conn, "a005 STATUS Trash (MESSAGES)\r\n")

	// Read STATUS responses
	statusResponse1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STATUS response 1: %v", err)
	}
	assert.Equal(t, "* STATUS Trash (MESSAGES 0)\r\n", statusResponse1)

	statusResponse2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read STATUS response 2: %v", err)
	}
	assert.Equal(t, "a005 OK STATUS completed\r\n", statusResponse2)

	// Send LOGOUT command
	fmt.Fprintf(conn, "a006 LOGOUT\r\n")

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
	assert.Equal(t, "a006 OK LOGOUT completed\r\n", ok)

	// Wait for the server to finish
	select {
	case <-serverDone:
		// Server finished normally
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for server to finish")
	}
}
