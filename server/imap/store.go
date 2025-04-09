package imap

import (
	"context"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

// Update flags for messages in the selected mailbox
func (s *IMAPSession) Store(w *imapserver.FetchWriter, seqSet imap.NumSet, numKind imapserver.NumKind, flags *imap.StoreFlags, options *imap.StoreOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.mailbox == nil {
		s.Log("Store failed: no mailbox selected")
		return &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeNonExistent,
			Text: "no mailbox selected",
		}
	}

	seqSet = s.mailbox.decodeNumSet(seqSet)

	ctx := context.Background()
	messages, err := s.server.db.GetMessagesBySeqSet(ctx, s.mailbox.ID, numKind, seqSet)
	if err != nil {
		return s.internalError("failed to retrieve messages: %v", err)
	}

	for _, msg := range messages {
		var newFlags *[]imap.Flag
		switch flags.Op {
		case imap.StoreFlagsAdd:
			newFlags, err = s.server.db.AddMessageFlags(ctx, msg.UID, msg.MailboxID, flags.Flags)
		case imap.StoreFlagsDel:
			newFlags, err = s.server.db.RemoveMessageFlags(ctx, msg.UID, msg.MailboxID, flags.Flags)
		case imap.StoreFlagsSet:
			newFlags, err = s.server.db.SetMessageFlags(ctx, msg.UID, msg.MailboxID, flags.Flags)
		}

		if err != nil {
			return s.internalError("failed to update flags for message: %v", err)
		}

		m := w.CreateMessage(s.mailbox.sessionTracker.EncodeSeqNum(msg.Seq))
		if !flags.Silent {
			m.WriteFlags(*newFlags)
		}
		m.Close()
	}
	return nil
}
