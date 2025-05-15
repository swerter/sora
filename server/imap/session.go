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
	if s.IMAPUser != nil {
		s.Log("Closing session for user: %v", s.FullAddress())
		s.IMAPUser = nil
	}
	s.mailbox = nil
	if s.cancel != nil {
		s.cancel() // Cancel the context when the session is closed
	}
	return nil
}
