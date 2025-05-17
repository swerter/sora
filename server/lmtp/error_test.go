package lmtp

import (
	"testing"

	"github.com/emersion/go-smtp"
	"github.com/stretchr/testify/assert"
)

// TestLMTPErrorCreation verifies that SMTP errors can be created correctly
func TestLMTPErrorCreation(t *testing.T) {
	// Create a new LMTP session
	session := &LMTPSession{}
	session.Session.Protocol = "LMTP"
	session.Session.Id = "test-session"

	// Test creating an internal error
	err := session.InternalError("test error: %v", "reason")

	// Verify that the error was created correctly
	assert.Error(t, err)
	smtpErr, ok := err.(*smtp.SMTPError)
	assert.True(t, ok)
	assert.Equal(t, 421, smtpErr.Code)
	assert.Equal(t, smtp.EnhancedCode{4, 4, 2}, smtpErr.EnhancedCode)
	assert.Equal(t, "test error: reason", smtpErr.Message)
}
