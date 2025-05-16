package imap

import (
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/db"
)

func (s *IMAPSession) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	s.mutex.Lock()
	if s.mailbox == nil {
		s.mutex.Unlock()
		return nil
	}
	mailbox := s.mailbox
	s.mutex.Unlock()

	ctx := s.Context()

	mailbox.Lock()
	defer mailbox.Unlock()

	poll, err := s.server.db.PollMailbox(ctx, mailbox.ID, mailbox.highestModSeq)
	if err != nil {
		return s.internalError("failed to poll mailbox: %v", err)
	}

	var numExpunged uint32
	for _, update := range poll.Updates {
		if update.IsExpunge {
			mailbox.mboxTracker.QueueExpunge(update.SeqNum)
			numExpunged++
		} else {
			flags := db.BitwiseToFlags(update.BitwiseFlags)
			mailbox.mboxTracker.QueueMessageFlags(update.SeqNum, update.UID, flags, nil)
		}
	}

	expectedCount := mailbox.numMessages - numExpunged

	if uint32(poll.NumMessages) > expectedCount {
		// Messages were added — safe to update
		mailbox.mboxTracker.QueueNumMessages(uint32(poll.NumMessages))
		mailbox.numMessages = poll.NumMessages

	} else if uint32(poll.NumMessages) < expectedCount {
		// Dangerous: messages disappeared without expunge — keep old count
		// Do NOT call QueueNumMessages() in this case
		s.Log("Warning: poll.NumMessages (%d) < expectedCount (%d); skipping QueueNumMessages to avoid panic",
			poll.NumMessages, expectedCount)
	} else {
		// Counts match, update safely
		mailbox.numMessages = poll.NumMessages
	}

	// Only update highestModSeq, numMessages is already handled in the conditions above
	mailbox.highestModSeq = poll.ModSeq

	return mailbox.sessionTracker.Poll(w, allowExpunge)
}
