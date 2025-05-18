package imap

import (
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/db"
)

// Poll checks for updates to the mailbox since the last poll and notifies clients of changes.
// This includes new messages, flag changes, and expunged messages.
//
// Parameters:
// - w: The UpdateWriter to send notifications to clients
// - allowExpunge: Whether to allow expunge notifications to be sent
//
// The function performs the following steps:
// 1. Safely get the current mailbox
// 2. Query the database for changes since the last poll
// 3. Process updates (expunges and flag changes)
// 4. Handle message count reconciliation
// 5. Update the highest modification sequence
// 6. Send notifications to clients
func (s *IMAPSession) Poll(w *imapserver.UpdateWriter, allowExpunge bool) error {
	// Safely get the current mailbox with proper locking
	s.mutex.Lock()
	if s.mailbox == nil {
		s.mutex.Unlock()
		return nil // No mailbox selected, nothing to poll
	}
	mailbox := s.mailbox
	s.mutex.Unlock()

	ctx := s.Context()

	s.Log("Polling mailbox %s (ID=%d) with highestModSeq=%d", mailbox.Name, mailbox.ID, mailbox.highestModSeq)

	// Lock the mailbox for the duration of the poll operation
	mailbox.Lock()
	defer mailbox.Unlock()

	// Query the database for changes since the last poll
	poll, err := s.server.db.PollMailbox(ctx, mailbox.ID, mailbox.highestModSeq)
	if err != nil {
		return s.internalError("failed to poll mailbox: %v", err)
	}

	// Log the poll results for debugging
	s.Log("Poll results: Updates=%d, NumMessages=%d, ModSeq=%d",
		len(poll.Updates), poll.NumMessages, poll.ModSeq)

	// Counters for tracking changes
	var numExpunged uint32
	var numUpdated uint32

	// Process each update (expunges and flag changes)
	for _, update := range poll.Updates {
		if update.IsExpunge {
			// Handle expunged messages
			mailbox.mboxTracker.QueueExpunge(update.SeqNum)
			numExpunged++
			s.Log("Expunged message: UID=%d, SeqNum=%d", update.UID, update.SeqNum)
		} else {
			// Handle flag changes
			flags := db.BitwiseToFlags(update.BitwiseFlags)
			mailbox.mboxTracker.QueueMessageFlags(update.SeqNum, update.UID, flags, nil)
			numUpdated++
			s.Log("Updated message: UID=%d, SeqNum=%d, Flags=%v", update.UID, update.SeqNum, flags)
		}
	}

	// Log summary of updates if any were processed
	if len(poll.Updates) > 0 {
		s.Log("Poll update summary: %d expunged, %d updated", numExpunged, numUpdated)
	}

	// Calculate the expected message count after processing expunges
	expectedCount := mailbox.numMessages - numExpunged

	// Message count reconciliation logic
	// This handles three cases:
	// 1. New messages were added (poll.NumMessages > expectedCount)
	// 2. Messages disappeared without expunge (poll.NumMessages < expectedCount)
	// 3. Counts match (poll.NumMessages == expectedCount)
	if uint32(poll.NumMessages) > expectedCount {
		// Case 1: Messages were added — safe to update
		s.Log("Poll detected new messages: poll.NumMessages=%d, expectedCount=%d, difference=%d",
			poll.NumMessages, expectedCount, uint32(poll.NumMessages)-expectedCount)
		mailbox.mboxTracker.QueueNumMessages(uint32(poll.NumMessages))
		mailbox.numMessages = uint32(poll.NumMessages)
	} else if uint32(poll.NumMessages) < expectedCount {
		// Case 2: Dangerous situation - messages disappeared without expunge
		// Do NOT call QueueNumMessages() in this case to avoid potential panic
		s.Log("Warning: poll.NumMessages (%d) < expectedCount (%d); skipping QueueNumMessages to avoid panic",
			poll.NumMessages, expectedCount)
	} else {
		// Case 3: Counts match, update safely
		s.Log("Message count unchanged: %d", poll.NumMessages)
		mailbox.numMessages = uint32(poll.NumMessages)
	}

	// Update the highest modification sequence
	oldModSeq := mailbox.highestModSeq
	mailbox.highestModSeq = poll.ModSeq

	if poll.ModSeq > oldModSeq {
		s.Log("Updated highestModSeq: %d -> %d", oldModSeq, poll.ModSeq)
	}

	// Perform the actual poll operation to send notifications to clients
	return mailbox.sessionTracker.Poll(w, allowExpunge)
}
