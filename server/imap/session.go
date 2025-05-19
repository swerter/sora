package imap

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	_ "github.com/emersion/go-message/charset"
	"github.com/migadu/sora/server"
)

type IMAPSession struct {
	server.Session
	*IMAPUser
	server  *IMAPServer
	conn    *imapserver.Conn
	ctx     context.Context
	cancel  context.CancelFunc
	mailbox *Mailbox
}

func (s *IMAPSession) Context() context.Context {
	return s.ctx
}

func (s *IMAPSession) internalError(format string, a ...interface{}) *imap.Error {
	s.Log(format, a...)
	return &imap.Error{
		Type: imap.StatusResponseTypeNo,
		Code: imap.ResponseCodeServerBug,
		Text: fmt.Sprintf(format, a...),
	}
}

func (s *IMAPSession) Close() error {
	// Check if s is nil
	if s == nil {
		return nil
	}

	// Safely handle IMAPUser with proper locking
	if s.IMAPUser != nil {
		userMutex := &s.IMAPUser.mutex
		fullAddress := s.IMAPUser.FullAddress()

		userMutex.Lock()
		s.Log("Closing session for user: %v", fullAddress)
		s.IMAPUser = nil
		// Also clear the User field in the embedded Session struct
		s.Session.User = nil
		userMutex.Unlock()
	} else {
		// Log when a client connection drops before authentication
		s.Log("Client connection dropped (unauthenticated)")
	}

	s.mailbox = nil

	if s.cancel != nil {
		s.cancel() // Cancel the context for this session
	}

	return nil
}
