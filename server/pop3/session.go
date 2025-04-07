package pop3

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server"
)

type POP3Session struct {
	server.Session
	server         *POP3Server
	conn           *net.Conn    // Connection to the client
	*server.User                // User associated with the session
	authenticated  bool         // Flag to indicate if the user has been authenticated
	messages       []db.Message // List of messages in the mailbox as returned by the LIST command
	deleted        map[int]bool // Map of message IDs marked for deletion
	inboxMailboxID int64        // POP3 suppots only INBOX
	errorsCount    int          // Number of errors encountered during the session
}

func (s *POP3Session) handleConnection() {
	defer s.Close()
	reader := bufio.NewReader(*s.conn)
	writer := bufio.NewWriter(*s.conn)

	writer.WriteString("+OK POP3 server ready\r\n")
	writer.Flush()

	s.Log("[POP3] connected")

	ctx := context.Background()

	for {
		// Set a read deadline for the connection
		(*s.conn).SetReadDeadline(time.Now().Add(IDLE_TIMEOUT))

		line, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				writer.WriteString("-ERR Connection timed out due to inactivity\r\n")
				writer.Flush()
				s.Log("[POP3] timed out")
			} else {
				s.Log("[POP3] error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		// --------------------------------------------------------------------------------------------
		case "USER":
			if s.authenticated {
				if s.handleClientError(writer, "-ERR Already authenticated\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			// While POP3 accepts any kind of username, we will only accept email addresses
			address, err := server.NewAddress(parts[1])
			if err != nil {
				s.Log("[POP3] error: %v", err)
				if s.handleClientError(writer, fmt.Sprintf("-ERR %s\r\n", err.Error())) {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			userID, err := s.server.db.GetUserIDByAddress(ctx, address.FullAddress())
			if err != nil {
				if err == consts.ErrUserNotFound {
					if s.handleClientError(writer, fmt.Sprintf("-ERR %s\r\n", err.Error())) {
						// Close the connection if too many errors are encountered
						return
					}
					continue
				}
				s.Log("[POP3] USER error: %v", err)
				writer.WriteString("-ERR Internal server error\r\n")
				writer.Flush()
				continue
			}

			inboxMailboxID, err := s.server.db.GetMailboxByName(ctx, userID, consts.MAILBOX_INBOX)
			if err != nil {
				if err == consts.ErrMailboxNotFound {
					if s.handleClientError(writer, fmt.Sprintf("-ERR %s\r\n", err.Error())) {
						// Close the connection if too many errors are encountered
						return
					}
					continue
				}
				s.Log("USER error: %v", err)
				writer.WriteString("-ERR Internal server error\r\n")
				writer.Flush()
				continue
			}

			s.inboxMailboxID = inboxMailboxID.ID
			s.User = server.NewUser(address, userID)
			writer.WriteString("+OK User accepted\r\n")

		// --------------------------------------------------------------------------------------------
		case "PASS":
			if s.authenticated {
				writer.WriteString("-ERR Already authenticated\r\n")
				writer.Flush()
				continue
			}

			s.Log("[POP3] authentication attempt")
			err := s.server.db.Authenticate(ctx, s.UserID(), parts[1])
			if err != nil {
				if s.handleClientError(writer, "-ERR Authentication failed\r\n") {
					s.Log("[POP3] authentication failed")
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}
			s.Log("[POP3] authenticated")
			s.authenticated = true
			writer.WriteString("+OK Password accepted\r\n")

		// --------------------------------------------------------------------------------------------
		case "STAT":
			messagesCount, size, err := s.server.db.GetMailboxMessageCountAndSizeSum(ctx, s.inboxMailboxID)
			if err != nil {
				s.Log("[POP3] STAT error: %v", err)
				writer.WriteString("-ERR Internal server error\r\n")
				writer.Flush()
				continue
			}
			writer.WriteString(fmt.Sprintf("+OK %d %d\r\n", messagesCount, size))

		// --------------------------------------------------------------------------------------------
		case "LIST":
			if !s.authenticated {
				if s.handleClientError(writer, "-ERR Not authenticated\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			s.messages, err = s.server.db.ListMessages(ctx, s.inboxMailboxID)
			if err != nil {
				s.Log("[POP3] LIST error: %v", err)
				writer.WriteString("-ERR Internal server error\r\n")
				writer.Flush()
				continue
			}

			if len(s.messages) == 0 {
				writer.WriteString("+OK 0 messages\r\n.\r\n")
			} else {
				writer.WriteString("+OK scan listing follows\r\n")
				for i, msg := range s.messages {
					if !s.deleted[i] {
						writer.WriteString(fmt.Sprintf("%d %d\r\n", i+1, msg.Size))
					}
				}
				writer.WriteString(".\r\n")
			}
			s.Log("[POP3] listed %d messages", len(s.messages))

		// --------------------------------------------------------------------------------------------
		case "RETR":
			if !s.authenticated {
				if s.handleClientError(writer, "-ERR Not authenticated\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			if len(parts) < 2 {
				if s.handleClientError(writer, "-ERR Missing message number\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			msgNumber, err := strconv.Atoi(parts[1])
			if err != nil || msgNumber < 1 {
				if s.handleClientError(writer, "-ERR Invalid message number\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			if s.messages == nil {
				s.messages, err = s.server.db.ListMessages(ctx, s.inboxMailboxID)
				if err != nil {
					s.Log("[POP3] RETR error: %v", err)
					writer.WriteString("-ERR Internal server error\r\n")
					writer.Flush()
					continue
				}
			}

			if msgNumber > len(s.messages) {
				if s.handleClientError(writer, "-ERR No such message\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			msg := s.messages[msgNumber-1]
			if msg == (db.Message{}) {
				if s.handleClientError(writer, "-ERR No such message\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			log.Printf("[POP3] Fetching message body for UID %d", msg.UID)
			bodyData, err := s.getMessageBody(&msg)
			if err != nil {
				s.Log("[POP3] RETR error: %v", err)
				writer.WriteString("-ERR Internal server error\r\n")
				writer.Flush()
				continue
			}
			s.Log("[POP3] retrieved message body for UID %d", msg.UID)

			writer.WriteString(fmt.Sprintf("+OK %d octets\r\n", msg.Size))
			writer.WriteString(string(bodyData))
			writer.WriteString("\r\n.\r\n")
			s.Log("[POP3] retrieved message %d", msg.UID)
		// --------------------------------------------------------------------------------------------
		case "NOOP":
			writer.WriteString("+OK\r\n")
		// --------------------------------------------------------------------------------------------
		case "RSET":
			s.deleted = make(map[int]bool)
			writer.WriteString("+OK\r\n")
			s.Log("[POP3] reset")
		// --------------------------------------------------------------------------------------------
		case "DELE":
			if !s.authenticated {
				if s.handleClientError(writer, "-ERR Not authenticated\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			if len(parts) < 2 {
				log.Printf("[POP3] Missing message number")
				if s.handleClientError(writer, "-ERR Missing message number\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			msgNumber, err := strconv.Atoi(parts[1])
			if err != nil || msgNumber < 1 {
				s.Log("[POP3] DELE error: %v", err)
				if s.handleClientError(writer, "-ERR Invalid message number\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			if s.messages == nil {
				s.messages, err = s.server.db.ListMessages(ctx, s.inboxMailboxID)
				if err != nil {
					s.Log("[POP3] DELE error: %v", err)
					writer.WriteString("-ERR Internal server error\r\n")
					writer.Flush()
					continue
				}
			}

			if msgNumber > len(s.messages) {
				s.Log("[POP3] DELE error: no such message %d", msgNumber)
				if s.handleClientError(writer, "-ERR No such message\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			msg := s.messages[msgNumber-1]
			if msg == (db.Message{}) {
				s.Log("[POP3] DELE error: no such message %d", msgNumber)
				if s.handleClientError(writer, "-ERR No such message\r\n") {
					// Close the connection if too many errors are encountered
					return
				}
				continue
			}

			s.deleted[msgNumber-1] = true
			writer.WriteString("+OK Message deleted\r\n")
			s.Log("[POP3] marked message %d for deletion", msg.UID)

		// --------------------------------------------------------------------------------------------
		case "QUIT":

			var expungeUIDs []imap.UID
			// Delete messages marked for deletion
			for i, deleted := range s.deleted {
				if deleted {
					s.Log("[POP3] expunging message %d", i)
					msg := s.messages[i]

					// Delete from cache before expunging
					err := s.server.cache.Delete(s.Domain(), s.LocalPart(), msg.UUID)
					if err != nil && !isNotExist(err) {
						s.Log("[POP3] Failed to delete message %s from cache: %v", msg.UUID.String(), err)
					}
					expungeUIDs = append(expungeUIDs, msg.UID)
				}
			}

			err = s.server.db.ExpungeMessageUIDs(ctx, s.inboxMailboxID, expungeUIDs...)
			if err != nil {
				s.Log("[POP3] error expunging messages: %v", err)
			}

			writer.WriteString("+OK Goodbye\r\n")
			writer.Flush()
			s.Close()
			return
		// --------------------------------------------------------------------------------------------
		default:
			writer.WriteString(fmt.Sprintf("-ERR Unknown command: %s\r\n", cmd))
			s.Log("[POP3] unknown command: %s", cmd)
		}
		writer.Flush()
	}
}

func isNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}

func (s *POP3Session) handleClientError(writer *bufio.Writer, errMsg string) bool {
	s.errorsCount++
	if s.errorsCount > MAX_ERRORS_ALLOWED {
		writer.WriteString("-ERR Too many errors, closing connection\r\n")
		writer.Flush()
		return true
	}
	// Make a delay to prevent brute force attacks
	time.Sleep(time.Duration(s.errorsCount) * ERROR_DELAY)
	writer.WriteString(errMsg)
	writer.Flush()
	return false
}

func (s *POP3Session) Close() error {
	(*s.conn).Close()
	if s.User != nil {
		s.Log("[POP3] closed")
		s.User = nil
		s.Id = ""
		s.messages = nil
		s.deleted = nil
		s.authenticated = false
	}
	return nil
}

func (s *POP3Session) getMessageBody(msg *db.Message) ([]byte, error) {
	if msg.S3Uploaded {
		// Try cache first
		data, err := s.server.cache.Get(s.Domain(), s.LocalPart(), msg.UUID)
		if err == nil && data != nil {
			log.Printf("[POP3][CACHE] Hit for UID %d", msg.UID)
			return data, nil
		}

		// Fallback to S3
		s3Key := server.S3Key(s.Domain(), s.LocalPart(), msg.UUID)
		log.Printf("[POP3][CACHE] Miss. Fetching UID %d from S3 (%s)", msg.UID, s3Key)
		reader, err := s.server.s3.GetMessage(s3Key)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve message UID %d from S3: %v", msg.UID, err)
		}
		defer reader.Close()
		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		_ = s.server.cache.Put(s.Domain(), s.LocalPart(), msg.UUID, data)
		return data, nil
	}

	// If not uploaded to S3, fetch from local disk
	log.Printf("[POP3] Fetching not yet uploaded message UID %d from disk", msg.UID)
	data, err := s.server.uploader.GetLocalFile(s.Domain(), s.LocalPart(), msg.UUID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve message UID %d from disk: %v", msg.UID, err)
	}
	if data == nil {
		return nil, fmt.Errorf("message UID %d not found on disk", msg.UID)
	}
	return data, nil
}
