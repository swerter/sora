package server

import (
	"strings"
	"testing"
)

func TestParseMessage_ValidMessage(t *testing.T) {
	validMsg := `From: sender@example.com
To: recipient@example.com
Subject: Test Message
Content-Type: text/plain

This is a test message.
`
	msg, err := ParseMessage(strings.NewReader(validMsg))
	if err != nil {
		t.Fatalf("ParseMessage failed for valid message: %v", err)
	}
	if msg == nil {
		t.Fatal("ParseMessage returned nil entity for valid message")
	}

	// Check basic headers are accessible
	from := msg.Header.Get("From")
	if from != "sender@example.com" {
		t.Errorf("Expected From: sender@example.com, got: %s", from)
	}
}

func TestParseMessage_MalformedMIMEHeader(t *testing.T) {
	// Create a message with a malformed MIME header (missing colon after header name)
	// This mimics the kind of corruption seen in production logs
	malformedMsg := "From: sender@example.com\r\n" +
		"To: recipient@example.com\r\n" +
		"Subject?\r\n" + // Invalid - missing colon
		"Content-Type: text/plain\r\n" +
		"\r\n" +
		"This message has a malformed header.\r\n"

	msg, err := ParseMessage(strings.NewReader(malformedMsg))

	// Should return an error for truly malformed MIME headers (we can't fix these)
	if err == nil {
		t.Fatal("ParseMessage should return error for truly malformed MIME headers")
	}

	if msg != nil {
		t.Error("ParseMessage should return nil entity for unrecoverable MIME errors")
	}
}

func TestParseMessage_CompletelyInvalidMIME(t *testing.T) {
	// Test with a message that has truly broken MIME structure
	brokenMsg := `From: sender@example.com
Subject?=` + "\r\n" + `
To: recipient@example.com

Body content
`
	msg, err := ParseMessage(strings.NewReader(brokenMsg))

	// Should return error for truly broken MIME
	if err == nil {
		t.Fatal("ParseMessage should return error for truly broken MIME")
	}

	if msg != nil {
		t.Error("ParseMessage should return nil entity for unrecoverable errors")
	}
}

func TestParseMessage_UnknownCharset(t *testing.T) {
	// Test that unknown charset errors are still handled gracefully (existing behavior)
	msgWithWeirdCharset := `From: sender@example.com
To: recipient@example.com
Subject: Test
Content-Type: text/plain; charset=unknown-charset-xyz

Body
`
	msg, err := ParseMessage(strings.NewReader(msgWithWeirdCharset))

	// Unknown charset should not cause an error
	if err != nil {
		t.Fatalf("ParseMessage should handle unknown charset, got error: %v", err)
	}

	if msg == nil {
		t.Fatal("ParseMessage returned nil entity")
	}
}

func TestParseMessage_MalformedEncoding(t *testing.T) {
	// Test the real-world case from production logs:
	// "encoding error: unhandled encoding "7BITquoted-printable""
	// This is a malformed Content-Transfer-Encoding that concatenates two valid values
	malformedEncodingMsg := `From: sender@example.com
To: recipient@example.com
Subject: Test with malformed encoding
Content-Type: text/plain
Content-Transfer-Encoding: 7BITquoted-printable

This message has a malformed Content-Transfer-Encoding header.
It should be either "7bit" or "quoted-printable", not concatenated.
`
	msg, err := ParseMessage(strings.NewReader(malformedEncodingMsg))

	// Should NOT return an error (graceful fallback via IsUnknownEncoding)
	if err != nil {
		t.Fatalf("ParseMessage should handle malformed encoding gracefully, got error: %v", err)
	}

	if msg == nil {
		t.Fatal("ParseMessage should return an entity even with malformed encoding")
	}

	// The message should still be readable (degraded mode)
	from := msg.Header.Get("From")
	if from != "sender@example.com" {
		t.Errorf("Expected From: sender@example.com, got: %s", from)
	}
}

func TestParseMessage_MalformedContentTransferEncoding(t *testing.T) {
	// Real-world case from production logs:
	// "encoding error: unhandled encoding "7bit <center>""
	//
	// go-message's message.Read() treats unknown encodings as non-fatal via
	// IsUnknownEncoding, returning a degraded but usable entity. For multipart
	// messages where the malformed encoding is in a child part, message.Read()
	// succeeds and the error only surfaces during part iteration in ExtractPlaintextBody.
	tests := []struct {
		name          string
		message       string
		expectError   bool
		expectParsing bool
	}{
		{
			name: "malformed encoding with HTML tag at top level",
			message: "From: sender@example.com\r\n" +
				"To: recipient@example.com\r\n" +
				"Subject: Test\r\n" +
				"Content-Type: text/plain; charset=utf-8\r\n" +
				"Content-Transfer-Encoding: 7bit <center>\r\n" +
				"\r\n" +
				"Body text here.\r\n",
			expectError:   false,
			expectParsing: true, // IsUnknownEncoding returns degraded entity
		},
		{
			name: "malformed encoding with garbage at top level",
			message: "From: sender@example.com\r\n" +
				"To: recipient@example.com\r\n" +
				"Subject: Test\r\n" +
				"Content-Type: text/plain\r\n" +
				"Content-Transfer-Encoding: base64 garbage\r\n" +
				"\r\n" +
				"Body\r\n",
			expectError:   false,
			expectParsing: true,
		},
		{
			name: "multipart with malformed encoding in child part",
			message: "From: sender@example.com\r\n" +
				"To: recipient@example.com\r\n" +
				"Subject: Test\r\n" +
				"MIME-Version: 1.0\r\n" +
				"Content-Type: multipart/alternative; boundary=\"boundary123\"\r\n" +
				"\r\n" +
				"--boundary123\r\n" +
				"Content-Type: text/plain; charset=utf-8\r\n" +
				"Content-Transfer-Encoding: 7bit\r\n" +
				"\r\n" +
				"First part is fine.\r\n" +
				"--boundary123\r\n" +
				"Content-Type: text/html; charset=utf-8\r\n" +
				"Content-Transfer-Encoding: 7bit <center>\r\n" +
				"\r\n" +
				"<html><body>This part has malformed encoding</body></html>\r\n" +
				"--boundary123--\r\n",
			expectError:   false,
			expectParsing: true, // Top-level parses fine; child error handled by ExtractPlaintextBody
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := ParseMessage(strings.NewReader(tt.message))

			if tt.expectError && err == nil {
				t.Fatal("Expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Fatalf("Expected no error but got: %v", err)
			}

			if tt.expectParsing {
				if msg == nil {
					t.Fatal("Expected parsed message but got nil")
				}

				// Verify message is actually usable
				from := msg.Header.Get("From")
				if from != "sender@example.com" {
					t.Errorf("Expected From: sender@example.com, got: %s", from)
				}
			}
		})
	}
}
