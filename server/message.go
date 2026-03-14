package server

import (
	"fmt"
	"io"
	"strings"

	"github.com/emersion/go-message"
	"github.com/migadu/sora/logger"
)

// ParseMessage reads and parses the email message from an io.Reader.
// It handles common MIME defects gracefully:
//   - Unknown charsets: returns a degraded but usable entity
//   - Unknown/malformed Content-Transfer-Encoding: returns a degraded entity
//     (the go-message library treats these as non-fatal)
//   - Malformed MIME headers: returns an error (unrecoverable)
func ParseMessage(r io.Reader) (*message.Entity, error) {
	m, err := message.Read(r)
	if message.IsUnknownCharset(err) {
		logger.Debug("Unknown charset, using fallback", "error", err)
		// Continue with degraded entity - charset errors are non-fatal
	} else if message.IsUnknownEncoding(err) {
		logger.Warn("Unknown or malformed Content-Transfer-Encoding, using fallback", "error", err)
		// Continue with degraded entity - encoding errors are non-fatal.
		// This handles cases like "Content-Transfer-Encoding: 7bit <center>" at the
		// top level. For multipart messages where the malformed encoding is in a child
		// part, the error surfaces during part iteration in ExtractPlaintextBody instead.
	} else if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "malformed MIME header") {
			logger.Warn("Malformed MIME header detected, rejecting message", "error", err)
		}
		return nil, fmt.Errorf("failed to read message: %v", err)
	}

	return m, nil
}
