package lmtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/emersion/go-message"
	"github.com/emersion/go-message/mail"

	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-smtp"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/helpers"
	"github.com/migadu/sora/server"
	"github.com/migadu/sora/server/sieveengine"
)

// sendToExternalRelay sends a message to the external relay using TLS
func (s *LMTPSession) sendToExternalRelay(from string, to string, message []byte) error {
	if s.backend.externalRelay == "" {
		return fmt.Errorf("external relay not configured")
	}

	// Configure TLS with secure defaults
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Connect to the SMTP server using TLS
	c, err := smtp.DialTLS(s.backend.externalRelay, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to external relay with TLS: %w", err)
	}
	defer c.Close()

	// Set the sender and recipient
	if err := c.Mail(from, nil); err != nil {
		return fmt.Errorf("failed to set sender: %w", err)
	}
	if err := c.Rcpt(to, nil); err != nil {
		return fmt.Errorf("failed to set recipient: %w", err)
	}

	// Send the message body
	wc, err := c.Data()
	if err != nil {
		return fmt.Errorf("failed to start data: %w", err)
	}
	_, err = wc.Write(message)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}
	err = wc.Close()
	if err != nil {
		return fmt.Errorf("failed to close data writer: %w", err)
	}

	// Send the QUIT command and close the connection
	err = c.Quit()
	if err != nil {
		return fmt.Errorf("failed to quit: %w", err)
	}

	return nil
}

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

	// Get the user's active script from the database
	activeScript, err := s.backend.db.GetActiveScript(ctx, s.UserID())

	// Create a default script in case we don't find an active one
	defaultScript := `require ["fileinto", "envelope", "reject"];
if envelope :is "from" "spam@example.com" {
    discard;
} elsif header :contains "subject" "important" {
    fileinto "Important";
} else {
    keep;
}`

	// Initialize variables for script evaluation result
	var result sieveengine.Result
	var mailboxName string

	// Create the sieve context (used for both default and user scripts)
	sieveCtx := sieveengine.Context{
		EnvelopeFrom: s.sender.FullAddress(),
		EnvelopeTo:   s.User.Address.FullAddress(),
		Header:       messageContent.Header.Map(),
		Body:         *plaintextBody,
	}

	// Always run the default script first as a "before script"
	defaultSieveExecutor, defaultScriptErr := sieveengine.NewSieveExecutor(defaultScript)
	if defaultScriptErr != nil {
		s.Log("[LMTP] Failed to create default sieve executor: %v", defaultScriptErr)
		// If we can't create a default sieve executor, use a simple keep action
		result = sieveengine.Result{Action: sieveengine.ActionKeep}
	} else {
		// Evaluate the default script
		defaultResult, defaultEvalErr := defaultSieveExecutor.Evaluate(sieveCtx)
		if defaultEvalErr != nil {
			s.Log("[LMTP] Default SIEVE evaluation error: %v", defaultEvalErr)
			// fallback: default to INBOX
			result = sieveengine.Result{Action: sieveengine.ActionKeep}
		} else {
			// Set the result from the default script
			result = defaultResult
			s.Log("[LMTP] Default script result action: %v", result.Action)
		}
	}

	// If user has an active script, run it and let it override the resultAction
	if err == nil && activeScript != nil {
		s.Log("[LMTP] Using user's active script: %s", activeScript.Name)
		userSieveExecutor, userScriptErr := sieveengine.NewSieveExecutor(activeScript.Script)
		if userScriptErr != nil {
			s.Log("[LMTP] Failed to create sieve executor from user script: %v", userScriptErr)
			// Keep the result from the default script
		} else {
			// Evaluate the user script
			userResult, userEvalErr := userSieveExecutor.Evaluate(sieveCtx)
			if userEvalErr != nil {
				s.Log("[LMTP] User SIEVE evaluation error: %v", userEvalErr)
				// Keep the result from the default script
			} else {
				// Override the result with the user script result
				result = userResult
				s.Log("[LMTP] User script overrode result action: %v", result.Action)
			}
		}
	} else {
		if err != nil && err != consts.ErrDBNotFound {
			s.Log("[LMTP] Failed to get active script: %v", err)
		} else {
			s.Log("[LMTP] No active script found, using only default script result")
		}
	}

	// Process the result
	switch result.Action {
	case sieveengine.ActionDiscard:
		s.Log("[LMTP] Message discarded by SIEVE")
		return nil
	case sieveengine.ActionFileInto:
		mailboxName = result.Mailbox
	case sieveengine.ActionRedirect:
		s.Log("[LMTP] Message redirected to %s by SIEVE", result.RedirectTo)

		// Send the message to the external relay if configured
		if s.backend.externalRelay != "" {
			err := s.sendToExternalRelay(s.sender.FullAddress(), result.RedirectTo, messageBytes)
			if err != nil {
				s.Log("[LMTP] Error sending redirected message to external relay: %v", err)
				// Continue processing even if relay fails, store in INBOX as fallback
			} else {
				s.Log("[LMTP] Successfully sent redirected message to %s via external relay %s",
					result.RedirectTo, s.backend.externalRelay)
				// Message was successfully relayed, no need to store it locally
				return nil
			}
		} else {
			s.Log("[LMTP] External relay not configured, storing message in INBOX")
		}

		// Fallback: store in INBOX if relay is not configured or fails
		mailboxName = consts.MAILBOX_INBOX
	case sieveengine.ActionVacation:
		// Handle vacation response
		err := s.handleVacationResponse(ctx, result, messageContent)
		if err != nil {
			s.Log("[LMTP] Error handling vacation response: %v", err)
			// Continue processing even if vacation response fails
		}
		// Store the original message in INBOX
		mailboxName = consts.MAILBOX_INBOX
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

// handleVacationResponse processes a vacation auto-response
func (s *LMTPSession) handleVacationResponse(ctx context.Context, result sieveengine.Result, originalMessage *message.Entity) error {
	// Check if we've already sent a vacation response to this sender recently
	// Default duration is 7 days if not specified in the sieve script
	duration := 7 * 24 * time.Hour

	// Check if we've already sent a response to this sender within the duration
	hasRecent, err := s.backend.db.HasRecentVacationResponse(ctx, s.UserID(), s.sender.FullAddress(), duration)
	if err != nil {
		return fmt.Errorf("failed to check recent vacation responses: %w", err)
	}

	// If we've already sent a response recently, don't send another one
	if hasRecent {
		s.Log("[LMTP] Skipping vacation response to %s (already sent recently)", s.sender.FullAddress())
		return nil
	}

	// Create the vacation response message
	var vacationFrom string
	if result.VacationFrom != "" {
		vacationFrom = result.VacationFrom
	} else {
		vacationFrom = s.User.Address.FullAddress()
	}

	var vacationSubject string
	if result.VacationSubj != "" {
		vacationSubject = result.VacationSubj
	} else {
		vacationSubject = "Auto: Out of Office"
	}

	// Get the original message ID for the In-Reply-To header
	originalHeader := mail.Header{Header: originalMessage.Header}
	originalMessageID, _ := originalHeader.MessageID()

	// Create a new message
	var vacationMessage bytes.Buffer

	// Create message header
	var h message.Header
	h.Set("From", vacationFrom)
	h.Set("To", s.sender.FullAddress())
	h.Set("Subject", vacationSubject)
	h.Set("Message-ID", fmt.Sprintf("<%d.vacation@%s>", time.Now().UnixNano(), s.HostName))
	if originalMessageID != "" {
		h.Set("In-Reply-To", originalMessageID)
		h.Set("References", originalMessageID)
	}
	h.Set("Auto-Submitted", "auto-replied")
	h.Set("X-Auto-Response-Suppress", "All")
	h.Set("Date", time.Now().Format(time.RFC1123Z))

	// Create the message with the header
	w, err := message.CreateWriter(&vacationMessage, h)
	if err != nil {
		return fmt.Errorf("failed to create message writer: %w", err)
	}

	// Create the text part
	var textHeader message.Header
	textHeader.Set("Content-Type", "text/plain; charset=utf-8")
	textWriter, err := w.CreatePart(textHeader)
	if err != nil {
		return fmt.Errorf("failed to create text part: %w", err)
	}

	// Write the vacation message body
	_, err = textWriter.Write([]byte(result.VacationMsg))
	if err != nil {
		return fmt.Errorf("failed to write vacation message body: %w", err)
	}

	// Close the writers
	textWriter.Close()
	w.Close()

	// Send the vacation response via the external relay if configured
	if s.backend.externalRelay != "" {
		err = s.sendToExternalRelay(vacationFrom, s.sender.FullAddress(), vacationMessage.Bytes())
		if err != nil {
			s.Log("[LMTP] Error sending vacation response via external relay: %v", err)
			// Continue processing even if relay fails
		} else {
			s.Log("[LMTP] Successfully sent vacation response to %s via external relay %s",
				s.sender.FullAddress(), s.backend.externalRelay)
		}
	} else {
		s.Log("[LMTP] External relay not configured, vacation response not sent")
	}

	// Record that we sent a vacation response to this sender
	err = s.backend.db.RecordVacationResponse(ctx, s.UserID(), s.sender.FullAddress())
	if err != nil {
		return fmt.Errorf("failed to record vacation response: %w", err)
	}

	return nil
}
