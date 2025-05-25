package imap

import (
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Copy(numSet imap.NumSet, mboxName string) (*imap.CopyData, error) {
	if s.selectedMailbox == nil {
		s.Log("[COPY] copy failed: no mailbox selected")
		return nil, &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeNonExistent,
			Text: "no mailbox selected",
		}
	}

	numSet = s.decodeNumSet(numSet)

	destMailbox, err := s.server.db.GetMailboxByName(s.ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("[COPY] copy failed: destination mailbox '%s' does not exist", mboxName)
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeNonExistent,
				Text: fmt.Sprintf("destination mailbox '%s' does not exist", mboxName),
			}
		}
		return nil, s.internalError("failed to fetch destination mailbox '%s': %v", mboxName, err)
	}

	messages, err := s.server.db.GetMessagesByNumSet(s.ctx, s.selectedMailbox.ID, numSet)
	if err != nil {
		return nil, s.internalError("failed to retrieve messages for copy: %v", err)
	}

	var sourceUIDs imap.UIDSet
	var destUIDs imap.UIDSet

	if len(messages) == 0 {
		return nil, nil
	}

	for _, msg := range messages {
		sourceUIDs.AddNum(msg.UID)
		copiedUID, err := s.server.db.InsertMessageCopy(s.ctx, msg.UID, msg.MailboxID, destMailbox.ID, destMailbox.Name)
		if err != nil {
			return nil, s.internalError("failed to insert copied message: %v", err)
		}
		destUIDs.AddNum(imap.UID(copiedUID))
	}

	if len(sourceUIDs) == 0 || len(destUIDs) == 0 {
		return nil, nil
	}

	copyData := &imap.CopyData{
		UIDValidity: destMailbox.UIDValidity,
		SourceUIDs:  sourceUIDs,
		DestUIDs:    destUIDs,
	}

	s.Log("[COPY] messages copied from %s to %s", s.selectedMailbox.Name, mboxName)

	return copyData, nil
}
