package imap

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	_ "github.com/emersion/go-message/charset"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server"
)

type IMAPSession struct {
	server.Session
	*IMAPUser
	server *IMAPServer
	conn   *imapserver.Conn
	ctx    context.Context
	cancel context.CancelFunc

	selectedMailbox *db.DBMailbox
	mailboxTracker  *imapserver.MailboxTracker
	sessionTracker  *imapserver.SessionTracker

	currentHighestModSeq uint64
	currentNumMessages   uint32
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
	if s == nil {
		return nil
	}

	if s.IMAPUser != nil {
		userMutex := &s.IMAPUser.mutex
		fullAddress := s.IMAPUser.FullAddress()

		userMutex.Lock()
		s.Log("closing session for user: %v", fullAddress)
		s.IMAPUser = nil
		s.Session.User = nil
		userMutex.Unlock()
	} else {
		s.Log("client dropped unauthenticated connection")
	}

	s.clearSelectedMailboxState()

	if s.cancel != nil {
		s.cancel()
	}

	return nil
}

func (s *IMAPSession) clearSelectedMailboxState() {
	if s.sessionTracker != nil {
		s.sessionTracker.Close()
	}
	s.selectedMailbox = nil
	s.mailboxTracker = nil
	s.sessionTracker = nil
	s.currentHighestModSeq = 0
	s.currentNumMessages = 0
}

func (s *IMAPSession) decodeNumSet(numSet imap.NumSet) imap.NumSet {
	if s.sessionTracker == nil {
		return numSet
	}

	seqSet, ok := numSet.(imap.SeqSet)
	if !ok {
		return numSet
	}

	var out imap.SeqSet
	for _, seqRange := range seqSet {
		start := s.sessionTracker.DecodeSeqNum(seqRange.Start)
		stop := s.sessionTracker.DecodeSeqNum(seqRange.Stop)

		if start == 0 && seqRange.Start != 0 {
			continue
		}
		if stop == 0 && seqRange.Stop != 0 {
			continue
		}
		out = append(out, imap.SeqRange{Start: start, Stop: stop})
	}
	if len(out) == 0 && len(seqSet) > 0 {
		return imap.SeqSet{}
	}
	return out
}

func (s *IMAPSession) PermittedFlags() []imap.Flag {
	if s.selectedMailbox == nil {
		return []imap.Flag{}
	}
	return []imap.Flag{
		imap.FlagSeen, imap.FlagAnswered, imap.FlagFlagged,
		imap.FlagDeleted, imap.FlagDraft,
	}
}
