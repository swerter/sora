package lmtp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/emersion/go-message/mail"

	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-smtp"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/helpers"
	"github.com/migadu/sora/server"
	"github.com/migadu/sora/server/sieveengine"
)

// LMTPSession represents a single LMTP session.
type LMTPSession struct {
	server.Session
	backend *LMTPServerBackend
	sender  *server.Address
	conn    *smtp.Conn
	cancel  context.CancelFunc // Function to cancel the session's context
	ctx     context.Context    // Context for this session, derived from server's appCtx
}

// Context returns the session's context.
func (s *LMTPSession) Context() context.Context {
	return s.ctx
}

func (s *LMTPSession) Mail(from string, opts *smtp.MailOptions) error {
	fromAddress, err := server.NewAddress(from)
	if err != nil {
		s.Log("Invalid from address: %v", err)
		return &smtp.SMTPError{
			Code:         553,
			EnhancedCode: smtp.EnhancedCode{5, 1, 7},
			Message:      "Invalid sender",
		}
	}
	s.sender = &fromAddress
	s.Log("Mail from=%s", fromAddress.FullAddress())
	return nil
}

func (s *LMTPSession) Rcpt(to string, opts *smtp.RcptOptions) error {
	toAddress, err := server.NewAddress(to)
	if err != nil {
		s.Log("Invalid to address: %v", err)
		return &smtp.SMTPError{
			Code:         513,
			EnhancedCode: smtp.EnhancedCode{5, 0, 1}, // TODO: Check the correct code
			Message:      "Invalid recipient",
		}
	}
	userId, err := s.backend.db.GetUserIDByAddress(context.Background(), toAddress.FullAddress())
	if err != nil {
		s.Log("Failed to get user ID by address: %v", err)
		return &smtp.SMTPError{
			Code:         550,
			EnhancedCode: smtp.EnhancedCode{5, 0, 2}, // TODO: Check the correct code
			Message:      "No such user here",
		}
	}
	s.User = server.NewUser(toAddress, userId)
	s.Log("Rcpt to=%s (UserID: %d)", toAddress.FullAddress(), userId)
	return nil
}

func (s *LMTPSession) Data(r io.Reader) error {
	// Read the entire message into a buffer
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		return s.internalError("failed to read message: %v", err)
	}

	// Trim the message to remove any leading CRLF characters
	messageBytes := bytes.TrimLeft(buf.Bytes(), "\r\n")

	messageContent, err := server.ParseMessage(bytes.NewReader(messageBytes))
	if err != nil {
		return s.internalError("failed to parse message: %v", err)
	}

	// Generate the content hash of the message
	contentHash := helpers.HashContent(messageBytes)

	// Parse message headers (this does not consume the body)
	mailHeader := mail.Header{Header: messageContent.Header}
	subject, _ := mailHeader.Subject()
	messageID, _ := mailHeader.MessageID()
	sentDate, _ := mailHeader.Date()
	inReplyTo, _ := mailHeader.MsgIDList("In-Reply-To")

	if sentDate.IsZero() {
		sentDate = time.Now()
	}

	bodyStructure := imapserver.ExtractBodyStructure(bytes.NewReader(buf.Bytes()))

	plaintextBody, err := helpers.ExtractPlaintextBody(messageContent)
	if err != nil {
		log.Printf("Failed to extract plaintext body: %v", err)
		// The plaintext body is needed only for indexing, so we can ignore the error
	}

	recipients := helpers.ExtractRecipients(messageContent.Header)

	filePath, err := s.backend.uploader.StoreLocally(contentHash, messageBytes)
	if err != nil {
		return s.internalError("failed to save message to disk: %v", err)
	}

	ctx := s.Context() // Use the session's context for operations

	sieveCtx := sieveengine.Context{
		EnvelopeFrom: s.sender.FullAddress(),
		EnvelopeTo:   s.User.Address.FullAddress(),
		Header:       messageContent.Header.Map(),
		Body:         *plaintextBody,
	}

	result, err := s.backend.sieve.Evaluate(sieveCtx)
	if err != nil {
		s.Log("[LMTP] SIEVE evaluation error: %v", err)
		// fallback: default to INBOX
		result = sieveengine.Result{Action: sieveengine.ActionKeep}
	}

	var mailboxName string
	switch result.Action {
	case sieveengine.ActionDiscard:
		s.Log("[LMTP] Message discarded by SIEVE")
		return nil
	case sieveengine.ActionFileInto:
		mailboxName = result.Mailbox
	default:
		mailboxName = consts.MAILBOX_INBOX
	}

	// Try to get the mailbox by name, if it fails, fallback to INBOX
	mailbox, err := s.backend.db.GetMailboxByName(ctx, s.UserID(), mailboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("Mailbox '%s' not found, falling back to INBOX", mailboxName)
			mailbox, err = s.backend.db.GetMailboxByName(ctx, s.UserID(), consts.MAILBOX_INBOX)
			if err != nil {
				return s.internalError("failed to get INBOX mailbox: %v", err)
			}
		} else {
			return s.internalError("failed to get mailbox '%s': %v", mailboxName, err)
		}
	}

	_, messageUID, err := s.backend.db.InsertMessage(ctx,
		&db.InsertMessageOptions{
			UserID:        s.UserID(),
			MailboxID:     mailbox.ID,
			MailboxName:   mailbox.Name,
			ContentHash:   contentHash, // The S3 key is the content hash of the message
			MessageID:     messageID,
			InternalDate:  time.Now(),
			Size:          int64(len(messageBytes)),
			Subject:       subject,
			PlaintextBody: plaintextBody,
			SentDate:      sentDate,
			InReplyTo:     inReplyTo,
			BodyStructure: &bodyStructure,
			Recipients:    recipients,
		},
		db.PendingUpload{ // This struct is used to pass data to InsertMessage, not directly inserted into DB here
			ContentHash: contentHash,
			InstanceID:  s.backend.hostname,
			Size:        int64(len(messageBytes)),
		})
	if err != nil {
		_ = os.Remove(*filePath) // cleanup file on failure
		if err == consts.ErrDBUniqueViolation {
			// Message already exists, continue with the next message
			return &smtp.SMTPError{
				Code:         541,
				EnhancedCode: smtp.EnhancedCode{5, 0, 1}, // TODO: Check the correct code
				Message:      "Message already exists",
			}
		}
		return s.internalError("failed to save message: %v", err)
	}

	s.backend.uploader.NotifyUploadQueued()

	s.Log("Message saved with UID %d", messageUID)
	return nil
}

func (s *LMTPSession) Reset() {
	s.User = nil
	s.sender = nil
	s.Log("Reset")
}

func (s *LMTPSession) Logout() error {
	s.Log("Logout")
	if s.cancel != nil {
		s.cancel() // Call cancel on logout
	}
	//TODO: Do we really need to return an error to terminate the session?
	return &smtp.SMTPError{
		Code:         221,
		EnhancedCode: smtp.EnhancedCode{2, 0, 0},
		Message:      "Closing transmission channel",
	}
}

func (s *LMTPSession) internalError(format string, a ...interface{}) error {
	s.Log(format, a...)
	return &smtp.SMTPError{
		Code:         421,
		EnhancedCode: smtp.EnhancedCode{4, 4, 2},
		Message:      fmt.Sprintf(format, a...),
	}
}
