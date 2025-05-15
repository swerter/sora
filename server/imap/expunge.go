package imap

import (
	"context"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

func (s *IMAPSession) Expunge(w *imapserver.ExpungeWriter, uidSet *imap.UIDSet) error {
	ctx := context.Background()

	messages, err := s.server.db.GetMessagesByFlag(ctx, s.mailbox.ID, imap.FlagDeleted)
	if err != nil {
		return s.internalError("failed to fetch deleted messages: %v", err)
	}

	var expungeUIDs []imap.UID
	for _, msg := range messages {
		if uidSet != nil && !uidSet.Contains(msg.UID) {
			continue
		}

		// Send EXPUNGE response with the message's sequence number *before* deleting from DB
		if err := w.WriteExpunge(uint32(msg.Seq)); err != nil {
			// Log the error but continue, as failing to send one response shouldn't stop the whole expunge
			s.Log("Failed to send EXPUNGE response for UID %d (Seq %d): %v", msg.UID, msg.Seq, err)
		}

		expungeUIDs = append(expungeUIDs, msg.UID)
	}

	if err := s.server.db.ExpungeMessageUIDs(ctx, s.mailbox.ID, expungeUIDs...); err != nil {
		return s.internalError("failed to expunge messages: %v", err)
	}

	s.Log("Expunged %d messages from mailbox %s", len(expungeUIDs), s.mailbox.Name)
	return nil
}
