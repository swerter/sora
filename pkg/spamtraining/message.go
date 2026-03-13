package spamtraining

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/emersion/go-message"
)

// StripAttachments removes all attachments from a MIME message body,
// keeping only text/plain and text/html parts for spam training.
// Returns the modified message body without attachments.
func StripAttachments(messageBody []byte) ([]byte, error) {
	// Parse the message using go-message
	entity, err := message.Read(bytes.NewReader(messageBody))
	if err != nil {
		return nil, fmt.Errorf("failed to parse message: %w", err)
	}

	// If it's not multipart, return as-is (simple text message)
	if !entity.Header.Has("Content-Type") {
		return messageBody, nil
	}

	mediaType, _, _ := entity.Header.ContentType()
	if !strings.HasPrefix(mediaType, "multipart/") {
		// Single-part message, return as-is
		return messageBody, nil
	}

	// Process multipart message and extract only text parts
	multipartReader := entity.MultipartReader()
	if multipartReader == nil {
		// Not actually multipart, return original
		return messageBody, nil
	}

	// Collect text parts
	var textParts [][]byte
	hasTextPart := false

	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read multipart: %w", err)
		}

		partMediaType, _, _ := part.Header.ContentType()

		// Only include text/plain and text/html parts
		if partMediaType == "text/plain" || partMediaType == "text/html" {
			hasTextPart = true

			// Read part body
			partBody, err := io.ReadAll(part.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read part body: %w", err)
			}

			// Build this part with headers
			var partBuf bytes.Buffer
			fields := part.Header.Fields()
			for fields.Next() {
				partBuf.WriteString(fields.Key())
				partBuf.WriteString(": ")
				partBuf.WriteString(fields.Value())
				partBuf.WriteString("\r\n")
			}
			partBuf.WriteString("\r\n")
			partBuf.Write(partBody)

			textParts = append(textParts, partBuf.Bytes())
		}
	}

	// Build new message
	var buf bytes.Buffer

	// Write all headers except Content-Type
	fields := entity.Header.Fields()
	for fields.Next() {
		key := fields.Key()
		if key != "Content-Type" {
			buf.WriteString(key)
			buf.WriteString(": ")
			buf.WriteString(fields.Value())
			buf.WriteString("\r\n")
		}
	}

	// If no text parts found, create a simple text message
	if !hasTextPart {
		buf.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
		buf.WriteString("\r\n")
		buf.WriteString("[No text content - attachments stripped for spam training]\n")
		return buf.Bytes(), nil
	}

	// If only one text part, make it a simple message
	if len(textParts) == 1 {
		buf.Write(textParts[0])
		return buf.Bytes(), nil
	}

	// Multiple text parts - create multipart/alternative
	boundary := "spam-training-boundary"
	buf.WriteString("Content-Type: multipart/alternative; boundary=\"")
	buf.WriteString(boundary)
	buf.WriteString("\"\r\n\r\n")

	for _, part := range textParts {
		buf.WriteString("--")
		buf.WriteString(boundary)
		buf.WriteString("\r\n")
		buf.Write(part)
		buf.WriteString("\r\n")
	}

	buf.WriteString("--")
	buf.WriteString(boundary)
	buf.WriteString("--\r\n")

	return buf.Bytes(), nil
}
