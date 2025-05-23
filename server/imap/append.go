package imap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-message/mail"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/helpers"
	"github.com/migadu/sora/server"

	_ "github.com/emersion/go-message/charset"
)

func (s *IMAPSession) Append(mboxName string, r imap.LiteralReader, options *imap.AppendOptions) (*imap.AppendData, error) {
	ctx := context.Background()
	mailbox, err := s.server.db.GetMailboxByName(ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeNonExistent,
				Text: fmt.Sprintf("mailbox '%s' does not exist", mboxName),
			}
		}
		return nil, s.internalError("failed to fetch mailbox '%s': %v", mboxName, err)
	}

	// Read the entire message into a buffer
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		return nil, s.internalError("failed to read message: %v", err)
	}

	// Trim the message to remove any leading CRLF characters
	messageBytes := bytes.TrimLeft(buf.Bytes(), "\r\n")

	messageContent, err := server.ParseMessage(bytes.NewReader(messageBytes))
	if err != nil {
		return nil, s.internalError("failed to parse message: %v", err)
	}

	contentHash := helpers.HashContent(messageBytes)

	// Parse message headers (this does not consume the body)
	mailHeader := mail.Header{Header: messageContent.Header}
	subject, _ := mailHeader.Subject()
	messageID, _ := mailHeader.MessageID()
	sentDate, _ := mailHeader.Date()
	inReplyTo, _ := mailHeader.MsgIDList("In-Reply-To")

	if len(inReplyTo) == 0 {
		inReplyTo = nil
	}

	if sentDate.IsZero() {
		sentDate = options.Time
	}

	bodyStructure := imapserver.ExtractBodyStructure(bytes.NewReader(buf.Bytes()))

	plaintextBody, err := helpers.ExtractPlaintextBody(messageContent)
	if err != nil {
		log.Printf("Failed to extract plaintext body: %v", err)
		// Continue with the append operation even if plaintext body extraction fails,
		// it will default to an empty string if not present
	}

	recipients := helpers.ExtractRecipients(messageContent.Header)

	filePath, err := s.server.uploader.StoreLocally(contentHash, messageBytes)
	if err != nil {
		return nil, s.internalError("failed to save message to disk: %v", err)
	}

	size := int64(len(messageBytes))

	_, messageUID, err := s.server.db.InsertMessage(ctx,
		&db.InsertMessageOptions{
			UserID:        s.UserID(),
			MailboxID:     mailbox.ID,
			MailboxName:   mailbox.Name,
			ContentHash:   contentHash,
			MessageID:     messageID,
			Flags:         options.Flags,
			InternalDate:  options.Time,
			Size:          size,
			Subject:       subject,
			PlaintextBody: plaintextBody,
			SentDate:      sentDate,
			InReplyTo:     inReplyTo,
			BodyStructure: &bodyStructure,
			Recipients:    recipients,
		},
		db.PendingUpload{
			InstanceID:  s.server.hostname,
			ContentHash: contentHash,
			Size:        size,
		})
	if err != nil {
		_ = os.Remove(*filePath) // cleanup file on failure
		if errors.Is(err, consts.ErrDBUniqueViolation) {
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeAlreadyExists,
				Text: "message already exists",
			}
		}
		return nil, s.internalError("failed to insert message metadata: %v", err)
	}

	s.server.uploader.NotifyUploadQueued()

	return &imap.AppendData{
		UID:         imap.UID(messageUID),
		UIDValidity: mailbox.UIDValidity,
	}, nil
}
