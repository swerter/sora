package imap

import (
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/db"
)

func (s *IMAPSession) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	if s.mailbox == nil {
		return nil
	}

	ctx := s.Context()

	poll, err := s.server.db.PollMailbox(ctx, s.mailbox.ID, s.mailbox.highestModSeq)
	if err != nil {
		return s.internalError("failed to poll mailbox: %v", err)
	}

	var numExpunged uint32
	for _, update := range poll.Updates {
		if update.IsExpunge {
			s.mailbox.mboxTracker.QueueExpunge(update.SeqNum)
			numExpunged++
		} else {
			flags := db.BitwiseToFlags(update.BitwiseFlags)
			s.mailbox.mboxTracker.QueueMessageFlags(update.SeqNum, update.UID, flags, nil)
		}
	}

	expectedCount := s.mailbox.numMessages - numExpunged

	s.mailbox.Lock()
	defer s.mailbox.Unlock()

	if uint32(poll.NumMessages) > expectedCount {
		// Messages were added — safe to update
		s.mailbox.mboxTracker.QueueNumMessages(uint32(poll.NumMessages))
		s.mailbox.numMessages = poll.NumMessages

	} else if uint32(poll.NumMessages) < expectedCount {
		// Dangerous: messages disappeared without expunge — keep old count
		// Do NOT call QueueNumMessages() in this case
		s.Log("Warning: poll.NumMessages (%d) < expectedCount (%d); skipping QueueNumMessages to avoid panic",
			poll.NumMessages, expectedCount)
	} else {
		// Counts match, update safely
		s.mailbox.numMessages = poll.NumMessages
	}

	s.mailbox.numMessages = poll.NumMessages
	s.mailbox.highestModSeq = poll.ModSeq

	return s.mailbox.sessionTracker.Poll(w, allowExpunge)
}
