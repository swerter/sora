package imap

import (
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

func (s *IMAPSession) Store(w *imapserver.FetchWriter, numSet imap.NumSet, flags *imap.StoreFlags, options *imap.StoreOptions) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.selectedMailbox == nil {
		s.Log("[STORE] store failed: no mailbox selected")
		return &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeNonExistent,
			Text: "no mailbox selected",
		}
	}

	numSet = s.decodeNumSet(numSet)

	messages, err := s.server.db.GetMessagesByNumSet(s.ctx, s.selectedMailbox.ID, numSet)
	if err != nil {
		return s.internalError("failed to retrieve messages: %v", err)
	}

	var modifiedMessages []struct {
		seq    uint32
		uid    imap.UID
		flags  []imap.Flag
		modSeq int64
	}

	for _, msg := range messages {
		// // CONDSTORE: Skip messages whose mod-sequence is greater than the UNCHANGEDSINCE value
		// _, hasCondStore := s.server.caps[imap.CapCondStore]
		// if hasCondStore && options != nil && options.UnchangedSince > 0 {
		// 	var currentModSeq int64
		// 	currentModSeq = msg.CreatedModSeq

		// 	if msg.UpdatedModSeq != nil && *msg.UpdatedModSeq > currentModSeq {
		// 		currentModSeq = *msg.UpdatedModSeq
		// 	}

		// 	if msg.ExpungedModSeq != nil && *msg.ExpungedModSeq > currentModSeq {
		// 		currentModSeq = *msg.ExpungedModSeq
		// 	}

		// 	if uint64(currentModSeq) > options.UnchangedSince {
		// 		s.Log("[STORE] CONDSTORE: Skipping message UID %d with MODSEQ %d > UNCHANGEDSINCE %d",
		// 			msg.UID, currentModSeq, options.UnchangedSince)
		// 		continue
		// 	}
		// }

		var newFlags *[]imap.Flag
		switch flags.Op {
		case imap.StoreFlagsAdd:
			newFlags, err = s.server.db.AddMessageFlags(s.ctx, msg.UID, msg.MailboxID, flags.Flags)
		case imap.StoreFlagsDel:
			newFlags, err = s.server.db.RemoveMessageFlags(s.ctx, msg.UID, msg.MailboxID, flags.Flags)
		case imap.StoreFlagsSet:
			newFlags, err = s.server.db.SetMessageFlags(s.ctx, msg.UID, msg.MailboxID, flags.Flags)
		}

		if err != nil {
			return s.internalError("failed to update flags for message: %v", err)
		}

		uidSet := imap.UIDSet{{Start: msg.UID, Stop: msg.UID}}
		updatedMsgs, err := s.server.db.GetMessagesByNumSet(s.ctx, s.selectedMailbox.ID, uidSet)
		if err != nil {
			s.Log("[STORE] WARNING: failed to get updated message MODSEQ: %v", err)
			continue
		}

		if len(updatedMsgs) == 0 {
			s.Log("[STORE] WARNING: message UID %d not found after flag update", msg.UID)
			continue
		}

		updatedMsg := updatedMsgs[0]

		var highestModSeq int64
		highestModSeq = updatedMsg.CreatedModSeq

		if updatedMsg.UpdatedModSeq != nil && *updatedMsg.UpdatedModSeq > highestModSeq {
			highestModSeq = *updatedMsg.UpdatedModSeq
		}

		if updatedMsg.ExpungedModSeq != nil && *updatedMsg.ExpungedModSeq > highestModSeq {
			highestModSeq = *updatedMsg.ExpungedModSeq
		}

		s.Log("[STORE] operation updated message UID %d, new MODSEQ: %d", msg.UID, highestModSeq)

		modifiedMessages = append(modifiedMessages, struct {
			seq    uint32
			uid    imap.UID
			flags  []imap.Flag
			modSeq int64
		}{
			seq:    msg.Seq,
			uid:    msg.UID,
			flags:  *newFlags,
			modSeq: highestModSeq,
		})
	}

	if !flags.Silent {
		for _, modified := range modifiedMessages {
			m := w.CreateMessage(s.sessionTracker.EncodeSeqNum(modified.seq))

			m.WriteFlags(modified.flags)
			m.WriteUID(modified.uid)
			// m.WriteModSeq(uint64(modified.modSeq))

			if err := m.Close(); err != nil {
				s.Log("[STORE] WARNING: failed to close fetch response for message UID %d: %v",
					modified.uid, err)
			}
		}
	}

	return nil
}
