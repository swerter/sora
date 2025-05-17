package lmtp

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/emersion/go-message"
	"github.com/emersion/go-message/mail"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/emersion/go-smtp"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/helpers"
	"github.com/migadu/sora/server"
	"github.com/migadu/sora/server/sieveengine"
)

//go:embed default.sieve
var defaultSieveScript string

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
	s.Log("[LMTP] Processing MAIL FROM command: %s", from)
	fromAddress, err := server.NewAddress(from)
	if err != nil {
		s.Log("[LMTP] Invalid from address: %v", err)
		return &smtp.SMTPError{
			Code:         553,
			EnhancedCode: smtp.EnhancedCode{5, 1, 7},
			Message:      "Invalid sender",
		}
	}
	s.sender = &fromAddress
	s.Log("[LMTP] Mail from=%s accepted", fromAddress.FullAddress())
	return nil
}

func (s *LMTPSession) Rcpt(to string, opts *smtp.RcptOptions) error {
	s.Log("[LMTP] Processing RCPT TO command: %s", to)
	toAddress, err := server.NewAddress(to)
	if err != nil {
		s.Log("[LMTP] Invalid to address: %v", err)
		return &smtp.SMTPError{
			Code:         513,
			EnhancedCode: smtp.EnhancedCode{5, 0, 1}, // TODO: Check the correct code
			Message:      "Invalid recipient",
		}
	}
	s.Log("[LMTP] Looking up user ID for address: %s", toAddress.FullAddress())
	userId, err := s.backend.db.GetUserIDByAddress(context.Background(), toAddress.FullAddress())
	if err != nil {
		s.Log("[LMTP] Failed to get user ID by address: %v", err)
		return &smtp.SMTPError{
			Code:         550,
			EnhancedCode: smtp.EnhancedCode{5, 0, 2}, // TODO: Check the correct code
			Message:      "No such user here",
		}
	}
	s.User = server.NewUser(toAddress, userId)
	s.Log("[LMTP] Recipient accepted: %s (UserID: %d)", toAddress.FullAddress(), userId)
	return nil
}

func (s *LMTPSession) Data(r io.Reader) error {
	s.Log("[LMTP] Processing DATA command")

	// Read the entire message into a buffer
	s.Log("[LMTP] Reading message data")
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r)
	if err != nil {
		return s.InternalError("[LMTP] Failed to read message: %v", err)
	}
	s.Log("[LMTP] Message data read successfully (%d bytes)", buf.Len())

	// Trim the message to remove any leading CRLF characters
	messageBytes := bytes.TrimLeft(buf.Bytes(), "\r\n")

	s.Log("[LMTP] Parsing message content")
	messageContent, err := server.ParseMessage(bytes.NewReader(messageBytes))
	if err != nil {
		return s.InternalError("[LMTP] Failed to parse message: %v", err)
	}
	s.Log("[LMTP] Message parsed successfully")

	// Generate the content hash of the message
	contentHash := helpers.HashContent(messageBytes)
	s.Log("[LMTP] Message content hash: %s", contentHash)

	// Parse message headers (this does not consume the body)
	mailHeader := mail.Header{Header: messageContent.Header}
	subject, _ := mailHeader.Subject()
	messageID, _ := mailHeader.MessageID()
	sentDate, _ := mailHeader.Date()
	inReplyTo, _ := mailHeader.MsgIDList("In-Reply-To")

	if sentDate.IsZero() {
		sentDate = time.Now()
		s.Log("[LMTP] No sent date found, using current time: %v", sentDate)
	} else {
		s.Log("[LMTP] Message sent date: %v", sentDate)
	}

	s.Log("[LMTP] Extracting body structure")
	bodyStructure := imapserver.ExtractBodyStructure(bytes.NewReader(buf.Bytes()))

	s.Log("[LMTP] Extracting plaintext body")
	plaintextBody, err := helpers.ExtractPlaintextBody(messageContent)
	if err != nil {
		s.Log("[LMTP] Failed to extract plaintext body: %v", err)
		// The plaintext body is needed only for indexing, so we can ignore the error
	}

	s.Log("[LMTP] Extracting recipients")
	recipients := helpers.ExtractRecipients(messageContent.Header)

	s.Log("[LMTP] Storing message locally")
	filePath, err := s.backend.uploader.StoreLocally(contentHash, messageBytes)
	if err != nil {
		return s.InternalError("[LMTP] Failed to save message to disk: %v", err)
	}
	s.Log("[LMTP] Message stored locally at: %s", *filePath)

	ctx := s.Context() // Use the session's context for operations

	// Get the user's active script from the database
	activeScript, err := s.backend.db.GetActiveScript(ctx, s.UserID())

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
	s.Log("[LMTP] Parsing default SIEVE script")
	s.Log("[LMTP] Default SIEVE script content: %s", defaultSieveScript)
	defaultSieveExecutor, defaultScriptErr := sieveengine.NewSieveExecutor(defaultSieveScript)
	if defaultScriptErr != nil {
		s.Log("[LMTP] Failed to create default sieve executor: %v", defaultScriptErr)
		// If we can't create a default sieve executor, use a simple keep action
		result = sieveengine.Result{Action: sieveengine.ActionKeep}
	} else {
		// Log headers for debugging
		s.Log("[LMTP] Message headers for SIEVE evaluation:")
		for key, values := range sieveCtx.Header {
			for _, value := range values {
				s.Log("[LMTP] Header: %s: %s", key, value)
			}
		}

		// Evaluate the default script
		s.Log("[LMTP] Evaluating default SIEVE script")
		defaultResult, defaultEvalErr := defaultSieveExecutor.Evaluate(sieveCtx)
		if defaultEvalErr != nil {
			s.Log("[LMTP] Default SIEVE evaluation error: %v", defaultEvalErr)
			// fallback: default to INBOX
			result = sieveengine.Result{Action: sieveengine.ActionKeep}
		} else {
			// Set the result from the default script
			result = defaultResult
			s.Log("[LMTP] Default script result action: %v", result.Action)

			// Log more details about the action
			switch result.Action {
			case sieveengine.ActionFileInto:
				s.Log("[LMTP] Default SIEVE fileinto: %s", result.Mailbox)
			case sieveengine.ActionRedirect:
				s.Log("[LMTP] Default SIEVE redirect: %s", result.RedirectTo)
			case sieveengine.ActionDiscard:
				s.Log("[LMTP] Default SIEVE discard")
			case sieveengine.ActionVacation:
				s.Log("[LMTP] Default SIEVE vacation response triggered")
			case sieveengine.ActionKeep:
				s.Log("[LMTP] Default SIEVE keep (deliver to INBOX)")
			}
		}
	}

	// If user has an active script, run it and let it override the resultAction
	if err == nil && activeScript != nil {
		s.Log("[LMTP] Using user's active script: %s", activeScript.Name)
		s.Log("[LMTP] Parsing user SIEVE script")
		s.Log("[LMTP] User SIEVE script content: %s", activeScript.Script)
		userSieveExecutor, userScriptErr := sieveengine.NewSieveExecutor(activeScript.Script)
		if userScriptErr != nil {
			s.Log("[LMTP] Failed to create sieve executor from user script: %v", userScriptErr)
			// Keep the result from the default script
		} else {
			// Evaluate the user script
			s.Log("[LMTP] Evaluating user SIEVE script")
			userResult, userEvalErr := userSieveExecutor.Evaluate(sieveCtx)
			if userEvalErr != nil {
				s.Log("[LMTP] User SIEVE evaluation error: %v", userEvalErr)
				// Keep the result from the default script
			} else {
				// Override the result with the user script result
				result = userResult
				s.Log("[LMTP] User script overrode result action: %v", result.Action)

				// Log more details about the action
				switch result.Action {
				case sieveengine.ActionFileInto:
					s.Log("[LMTP] User SIEVE fileinto: %s", result.Mailbox)
				case sieveengine.ActionRedirect:
					s.Log("[LMTP] User SIEVE redirect: %s", result.RedirectTo)
				case sieveengine.ActionDiscard:
					s.Log("[LMTP] User SIEVE discard")
				case sieveengine.ActionVacation:
					s.Log("[LMTP] User SIEVE vacation response triggered")
				case sieveengine.ActionKeep:
					s.Log("[LMTP] User SIEVE keep (deliver to INBOX)")
				}
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
	s.Log("[LMTP] Processing final SIEVE action: %v", result.Action)
	switch result.Action {
	case sieveengine.ActionDiscard:
		s.Log("[LMTP] Message discarded by SIEVE - message will not be delivered")
		return nil
	case sieveengine.ActionFileInto:
		mailboxName = result.Mailbox
		s.Log("[LMTP] SIEVE fileinto action - delivering message to mailbox: %s", mailboxName)
	case sieveengine.ActionRedirect:
		s.Log("[LMTP] SIEVE redirect action - redirecting message to: %s", result.RedirectTo)

		// Send the message to the external relay if configured
		if s.backend.externalRelay != "" {
			s.Log("[LMTP] Sending redirected message via external relay: %s", s.backend.externalRelay)
			err := s.sendToExternalRelay(s.sender.FullAddress(), result.RedirectTo, messageBytes)
			if err != nil {
				s.Log("[LMTP] Error sending redirected message to external relay: %v", err)
				s.Log("[LMTP] Falling back to local delivery to INBOX")
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
		s.Log("[LMTP] SIEVE vacation action - sending auto-response and delivering original message to INBOX")
		// Handle vacation response
		err := s.handleVacationResponse(ctx, result, messageContent)
		if err != nil {
			s.Log("[LMTP] Error handling vacation response: %v", err)
			// Continue processing even if vacation response fails
		}
		// Store the original message in INBOX
		mailboxName = consts.MAILBOX_INBOX
	default:
		s.Log("[LMTP] SIEVE keep action (default) - delivering message to INBOX")
		mailboxName = consts.MAILBOX_INBOX
	}

	// Try to get the mailbox by name, if it fails, fallback to INBOX
	s.Log("[LMTP] Looking up mailbox: %s", mailboxName)
	mailbox, err := s.backend.db.GetMailboxByName(ctx, s.UserID(), mailboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("[LMTP] Mailbox '%s' not found, falling back to INBOX", mailboxName)
			mailbox, err = s.backend.db.GetMailboxByName(ctx, s.UserID(), consts.MAILBOX_INBOX)
			if err != nil {
				return s.InternalError("[LMTP] Failed to get INBOX mailbox: %v", err)
			}
			s.Log("[LMTP] Successfully found INBOX mailbox (ID: %d)", mailbox.ID)
		} else {
			return s.InternalError("[LMTP] Failed to get mailbox '%s': %v", mailboxName, err)
		}
	} else {
		s.Log("[LMTP] Successfully found mailbox '%s' (ID: %d)", mailboxName, mailbox.ID)
	}

	// By default, new messages should be marked as unread (no \Seen flag)
	// The Flags field is explicitly set to an empty slice to ensure no flags are set
	s.Log("[LMTP] Inserting message into mailbox '%s' (ID: %d)", mailbox.Name, mailbox.ID)
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
			Flags:         []imap.Flag{}, // Explicitly set empty flags to mark as unread
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
			s.Log("[LMTP] Message already exists in database (content hash: %s)", contentHash)
			return &smtp.SMTPError{
				Code:         541,
				EnhancedCode: smtp.EnhancedCode{5, 0, 1}, // TODO: Check the correct code
				Message:      "Message already exists",
			}
		}
		return s.InternalError("[LMTP] Failed to save message: %v", err)
	}

	s.backend.uploader.NotifyUploadQueued()

	s.Log("[LMTP] Message successfully saved with UID %d in mailbox '%s'", messageUID, mailbox.Name)
	return nil
}

func (s *LMTPSession) Reset() {
	s.Log("[LMTP] Session reset requested")
	s.User = nil
	s.sender = nil
	s.Log("[LMTP] Session reset completed")
}

func (s *LMTPSession) Logout() error {
	// Check if this is a normal QUIT command or an abrupt connection close
	if s.conn != nil && s.conn.Conn() != nil {
		// If we can still access the connection, it's a normal QUIT
		s.Log("[LMTP] Session logout requested")
	} else {
		// If the connection is nil or already closed, it's an abrupt disconnect
		s.Log("[LMTP] Client dropped connection")
	}

	if s.cancel != nil {
		s.cancel() // Call cancel on logout
		s.Log("[LMTP] Session context canceled")
	}

	s.Log("[LMTP] Session logout completed")
	return &smtp.SMTPError{
		Code:         221,
		EnhancedCode: smtp.EnhancedCode{2, 0, 0},
		Message:      "Closing transmission channel",
	}
}

func (s *LMTPSession) InternalError(format string, a ...interface{}) error {
	s.Log(format, a...)
	errorMsg := fmt.Sprintf(format, a...)
	s.Log("[LMTP] Internal error: %s", errorMsg)
	return &smtp.SMTPError{
		Code:         421,
		EnhancedCode: smtp.EnhancedCode{4, 4, 2},
		Message:      errorMsg,
	}
}

// handleVacationResponse processes a vacation auto-response
func (s *LMTPSession) handleVacationResponse(ctx context.Context, result sieveengine.Result, originalMessage *message.Entity) error {
	s.Log("[LMTP] Processing vacation response for sender: %s", s.sender.FullAddress())

	// Check if we've already sent a vacation response to this sender recently
	// Default duration is 7 days if not specified in the sieve script
	duration := 7 * 24 * time.Hour
	s.Log("[LMTP] Checking for recent vacation responses within %v", duration)

	// Check if we've already sent a response to this sender within the duration
	hasRecent, err := s.backend.db.HasRecentVacationResponse(ctx, s.UserID(), s.sender.FullAddress(), duration)
	if err != nil {
		s.Log("[LMTP] Error checking recent vacation responses: %v", err)
		return fmt.Errorf("failed to check recent vacation responses: %w", err)
	}

	// If we've already sent a response recently, don't send another one
	if hasRecent {
		s.Log("[LMTP] Skipping vacation response to %s (already sent recently)", s.sender.FullAddress())
		return nil
	}

	s.Log("[LMTP] No recent vacation response found, creating new response")

	// Create the vacation response message
	var vacationFrom string
	if result.VacationFrom != "" {
		vacationFrom = result.VacationFrom
		s.Log("[LMTP] Using custom vacation from address: %s", vacationFrom)
	} else {
		vacationFrom = s.User.Address.FullAddress()
		s.Log("[LMTP] Using default vacation from address: %s", vacationFrom)
	}

	var vacationSubject string
	if result.VacationSubj != "" {
		vacationSubject = result.VacationSubj
		s.Log("[LMTP] Using custom vacation subject: %s", vacationSubject)
	} else {
		vacationSubject = "Auto: Out of Office"
		s.Log("[LMTP] Using default vacation subject: %s", vacationSubject)
	}

	// Get the original message ID for the In-Reply-To header
	originalHeader := mail.Header{Header: originalMessage.Header}
	originalMessageID, _ := originalHeader.MessageID()
	if originalMessageID != "" {
		s.Log("[LMTP] Using original message ID for In-Reply-To: %s", originalMessageID)
	}

	s.Log("[LMTP] Creating vacation response message")
	// Create a new message
	var vacationMessage bytes.Buffer

	// Create message header
	var h message.Header
	h.Set("From", vacationFrom)
	h.Set("To", s.sender.FullAddress())
	h.Set("Subject", vacationSubject)
	messageID := fmt.Sprintf("<%d.vacation@%s>", time.Now().UnixNano(), s.HostName)
	h.Set("Message-ID", messageID)
	s.Log("[LMTP] Vacation message ID: %s", messageID)

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
		s.Log("[LMTP] Error creating message writer: %v", err)
		return fmt.Errorf("failed to create message writer: %w", err)
	}

	// Create the text part
	var textHeader message.Header
	textHeader.Set("Content-Type", "text/plain; charset=utf-8")
	textWriter, err := w.CreatePart(textHeader)
	if err != nil {
		s.Log("[LMTP] Error creating text part: %v", err)
		return fmt.Errorf("failed to create text part: %w", err)
	}

	// Write the vacation message body
	s.Log("[LMTP] Adding vacation message body (length: %d bytes)", len(result.VacationMsg))
	_, err = textWriter.Write([]byte(result.VacationMsg))
	if err != nil {
		s.Log("[LMTP] Error writing vacation message body: %v", err)
		return fmt.Errorf("failed to write vacation message body: %w", err)
	}

	// Close the writers
	textWriter.Close()
	w.Close()
	s.Log("[LMTP] Vacation message created successfully")

	// Send the vacation response via the external relay if configured
	if s.backend.externalRelay != "" {
		s.Log("[LMTP] Sending vacation response via external relay: %s", s.backend.externalRelay)
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
	s.Log("[LMTP] Recording vacation response in database")
	err = s.backend.db.RecordVacationResponse(ctx, s.UserID(), s.sender.FullAddress())
	if err != nil {
		s.Log("[LMTP] Error recording vacation response: %v", err)
		return fmt.Errorf("failed to record vacation response: %w", err)
	}
	s.Log("[LMTP] Vacation response successfully recorded in database")

	return nil
}
