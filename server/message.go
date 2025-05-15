package server

import (
	"fmt"
	"io"
	"log"

	"github.com/emersion/go-message"
)

// ParseMessage reads and parses the email message from an io.Reader
func ParseMessage(r io.Reader) (*message.Entity, error) {
	// Read the message from the reader
	m, err := message.Read(r)
	if message.IsUnknownCharset(err) {
		log.Println("Unknown encoding:", err)
	} else if err != nil {
		return nil, fmt.Errorf("failed to read message: %v", err)
	}

	return m, nil
}
