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

	s.Log("Polling mailbox %s (ID=%d) with highestModSeq=%d", mailbox.Name, mailbox.ID, mailbox.highestModSeq)

	mailbox.Lock()
	defer mailbox.Unlock()

	poll, err := s.server.db.PollMailbox(ctx, mailbox.ID, mailbox.highestModSeq)
	if err != nil {
		return s.internalError("failed to poll mailbox: %v", err)
	}

	// Log the poll results
	s.Log("Poll results: Updates=%d, NumMessages=%d, ModSeq=%d",
		len(poll.Updates), poll.NumMessages, poll.ModSeq)

	var numExpunged uint32
	var numUpdated uint32

	// Process each update
	for _, update := range poll.Updates {
		if update.IsExpunge {
			mailbox.mboxTracker.QueueExpunge(update.SeqNum)
			numExpunged++
			s.Log("Expunged message: UID=%d, SeqNum=%d", update.UID, update.SeqNum)
		} else {
			flags := db.BitwiseToFlags(update.BitwiseFlags)
			mailbox.mboxTracker.QueueMessageFlags(update.SeqNum, update.UID, flags, nil)
			numUpdated++
			s.Log("Updated message: UID=%d, SeqNum=%d, Flags=%v", update.UID, update.SeqNum, flags)
		}
	}

	// Log summary of updates
	if len(poll.Updates) > 0 {
		s.Log("Poll update summary: %d expunged, %d updated", numExpunged, numUpdated)
	}

	expectedCount := mailbox.numMessages - numExpunged

	// Check if we need to update the message count
	if uint32(poll.NumMessages) > expectedCount {
		// Messages were added — safe to update
		s.Log("Poll detected new messages: poll.NumMessages=%d, expectedCount=%d, difference=%d",
			poll.NumMessages, expectedCount, uint32(poll.NumMessages)-expectedCount)
		mailbox.mboxTracker.QueueNumMessages(uint32(poll.NumMessages))
		mailbox.numMessages = uint32(poll.NumMessages)

	} else if uint32(poll.NumMessages) < expectedCount {
		// Dangerous: messages disappeared without expunge — keep old count
		// Do NOT call QueueNumMessages() in this case
		s.Log("Warning: poll.NumMessages (%d) < expectedCount (%d); skipping QueueNumMessages to avoid panic",
			poll.NumMessages, expectedCount)
	} else {
		// Counts match, update safely
		s.Log("Message count unchanged: %d", poll.NumMessages)
		mailbox.numMessages = uint32(poll.NumMessages)
	}

	// Update the highest modseq
	oldModSeq := mailbox.highestModSeq
	mailbox.highestModSeq = poll.ModSeq

	if poll.ModSeq > oldModSeq {
		s.Log("Updated highestModSeq: %d -> %d", oldModSeq, poll.ModSeq)
	}

	// Perform the actual poll operation
	return mailbox.sessionTracker.Poll(w, allowExpunge)
}
