package imap

import (
	"context"
	"os"

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

		// Delete from cache before expunging
		err := s.server.cache.Delete(s.Domain(), s.LocalPart(), msg.UUID)
		if err != nil && !isNotExist(err) {
			s.Log("Failed to delete message %s from cache: %v", msg.UUID.String(), err)
		}

		expungeUIDs = append(expungeUIDs, msg.UID)
	}

	if err := s.server.db.ExpungeMessageUIDs(ctx, s.mailbox.ID, expungeUIDs...); err != nil {
		return s.internalError("failed to expunge messages: %v", err)
	}

	s.Log("Expunged %d messages", len(expungeUIDs))
	return nil
}

func isNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}
