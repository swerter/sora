package imap

import (
	"context"
	"fmt"
	"strings"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Move(w *imapserver.MoveWriter, numSet imap.NumSet, dest string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ctx := context.Background()

	numSet = s.mailbox.decodeNumSet(numSet)

	// Find the destination mailbox by its name
	destMailbox, err := s.server.db.GetMailboxByName(ctx, s.UserID(), dest)
	if err != nil {
		return &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeNonExistent,
			Text: fmt.Sprintf("destination mailbox '%s' not found", dest),
		}
	}

	// Get messages by sequence or UID set (based on NumKind, which is SeqNum or UID)
	messages, err := s.server.db.GetMessagesBySeqSet(ctx, s.mailbox.ID, numSet)
	if err != nil {
		return s.internalError("failed to retrieve messages: %v", err)
	}

	// Collect message IDs for moving and sequence numbers for expunge
	var sourceUIDs []imap.UID
	var seqNums []uint32

	for _, msg := range messages {
		sourceUIDs = append(sourceUIDs, msg.UID)
		seqNums = append(seqNums, uint32(msg.Seq))
	}

	// Move messages in the database
	messageUIDMap, err := s.server.db.MoveMessages(ctx, &sourceUIDs, s.mailbox.ID, destMailbox.ID)
	if err != nil {
		return s.internalError("failed to move messages: %v", err)
	}

	// Prepare the source and destination UIDs for the COPYUID response
	var mappedSourceUIDs []imap.UID
	var mappedDestUIDs []imap.UID

	// messageUIDMap holds the mapping between original UIDs and new UIDs
	for originalUID, newUID := range messageUIDMap {
		mappedSourceUIDs = append(mappedSourceUIDs, imap.UID(originalUID))
		mappedDestUIDs = append(mappedDestUIDs, imap.UID(newUID))
	}

	// Prepare CopyData (UID data for the COPYUID response)
	copyData := &imap.CopyData{
		UIDValidity: s.mailbox.UIDValidity,               // UIDVALIDITY of the source mailbox
		SourceUIDs:  imap.UIDSetNum(mappedSourceUIDs...), // Original UIDs (source mailbox)
		DestUIDs:    imap.UIDSetNum(mappedDestUIDs...),   // New UIDs in the destination mailbox
	}

	// Write the CopyData (COPYUID response)
	if err := w.WriteCopyData(copyData); err != nil {
		return s.internalError("failed to write COPYUID: %v", err)
	}

	// Check if destination is Trash folder
	isTrashFolder := strings.EqualFold(dest, "Trash") || dest == consts.MAILBOX_TRASH
	if isTrashFolder && len(mappedDestUIDs) > 0 {
		s.Log("Automatically marking %d moved messages as seen in Trash folder", len(mappedDestUIDs))

		for _, uid := range mappedDestUIDs {
			_, err := s.server.db.AddMessageFlags(ctx, uid, destMailbox.ID, []imap.Flag{imap.FlagSeen})
			if err != nil {
				s.Log("Failed to mark message UID %d as seen in Trash: %v", uid, err)
				// Continue with other messages even if one fails
			}
		}
	}

	// Expunge messages in the source mailbox (optional)
	for _, seqNum := range seqNums {
		if err := w.WriteExpunge(s.mailbox.sessionTracker.EncodeSeqNum(seqNum)); err != nil {
			return s.internalError("failed to write EXPUNGE: %v", err)
		}
	}

	return nil
}
