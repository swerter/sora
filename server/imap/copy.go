package imap

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Copy(seqSet imap.NumSet, numKind imapserver.NumKind, mboxName string) (*imap.CopyData, error) {
	if s.mailbox == nil {
		s.Log("Copy failed: no mailbox selected")
		return nil, &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeNonExistent,
			Text: "no mailbox selected",
		}
	}

	ctx := context.Background()

	destMailbox, err := s.server.db.GetMailboxByName(ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("Copy failed: destination mailbox '%s' does not exist", mboxName)
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeNonExistent,
				Text: fmt.Sprintf("destination mailbox '%s' does not exist", mboxName),
			}
		}
		return nil, s.internalError("failed to fetch destination mailbox '%s': %v", mboxName, err)
	}

	messages, err := s.server.db.GetMessagesBySeqSet(ctx, s.mailbox.ID, numKind, seqSet)
	if err != nil {
		return nil, s.internalError("failed to retrieve messages for copy: %v", err)
	}

	var sourceUIDs imap.UIDSet
	var destUIDs imap.UIDSet
	for _, msg := range messages {
		sourceUIDs.AddNum(msg.UID)
		copiedUID, err := s.server.db.InsertMessageCopy(ctx, msg.UID, msg.MailboxID, destMailbox.ID, destMailbox.Name)
		if err != nil {
			return nil, s.internalError("failed to insert copied message: %v", err)
		}
		destUIDs.AddNum(imap.UID(copiedUID))
	}

	copyData := &imap.CopyData{
		UIDValidity: destMailbox.UIDValidity,
		SourceUIDs:  sourceUIDs,
		DestUIDs:    destUIDs,
	}

	s.Log("Messages copied from %s to %s", s.mailbox.Name, mboxName)

	return copyData, nil
}
