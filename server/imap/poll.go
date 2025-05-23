package imap

import (
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/db"
)

func (s *IMAPSession) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	if s.selectedMailbox == nil || s.mailboxTracker == nil || s.sessionTracker == nil {
		return nil
	}

	s.mutex.Lock()
	mailboxID := s.selectedMailbox.ID
	highestModSeqToPollFrom := s.currentHighestModSeq
	s.mutex.Unlock()

	poll, err := s.server.db.PollMailbox(s.ctx, mailboxID, highestModSeqToPollFrom)
	if err != nil {
		return s.internalError("failed to poll mailbox: %v", err)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.selectedMailbox == nil || s.selectedMailbox.ID != mailboxID || s.mailboxTracker == nil || s.sessionTracker == nil {
		return nil
	}

	s.currentHighestModSeq = poll.ModSeq

	for _, update := range poll.Updates {
		if update.IsExpunge {
			s.mailboxTracker.QueueExpunge(update.SeqNum)
			s.currentNumMessages = s.currentNumMessages - 1
		} else {
			flags := db.BitwiseToFlags(int(update.BitwiseFlags))
			s.mailboxTracker.QueueMessageFlags(update.SeqNum, update.UID, flags, nil)
		}
	}

	if poll.NumMessages > s.currentNumMessages {
		s.mailboxTracker.QueueNumMessages(poll.NumMessages)
		s.currentNumMessages = poll.NumMessages
	}

	return s.sessionTracker.Poll(w, allowExpunge)
}
