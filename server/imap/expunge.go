package imap

import (
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

func (s *IMAPSession) Expunge(w *imapserver.ExpungeWriter, uidSet *imap.UIDSet) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	messages, err := s.server.db.GetMessagesByFlag(s.ctx, s.selectedMailbox.ID, imap.FlagDeleted)

	if err != nil {
		return s.internalError("failed to fetch deleted messages: %v", err)
	}

	var messagesToExpunge []struct {
		uid imap.UID
		seq uint32
	}

	if uidSet != nil {
		for _, msg := range messages {
			if uidSet.Contains(msg.UID) {
				messagesToExpunge = append(messagesToExpunge, struct {
					uid imap.UID
					seq uint32
				}{uid: msg.UID, seq: msg.Seq})
			}
		}
	} else {
		for _, msg := range messages {
			messagesToExpunge = append(messagesToExpunge, struct {
				uid imap.UID
				seq uint32
			}{uid: msg.UID, seq: msg.Seq})
		}
	}

	if len(messagesToExpunge) == 0 {
		return nil
	}

	var uidsToDelete []imap.UID
	for _, m := range messagesToExpunge {
		uidsToDelete = append(uidsToDelete, m.uid)
	}

	if err := s.server.db.ExpungeMessageUIDs(s.ctx, s.selectedMailbox.ID, uidsToDelete...); err != nil {
		return s.internalError("failed to expunge messages: %v", err)
	}

	for _, m := range messagesToExpunge {
		sessionSeqNum := s.sessionTracker.EncodeSeqNum(m.seq)
		if sessionSeqNum > 0 {
			if err := w.WriteExpunge(sessionSeqNum); err != nil {
				s.Log("[EXPUNGE] Error writing expunge for sessionSeqNum %d (UID %d, dbSeq %d): %v", sessionSeqNum, m.uid, m.seq, err)
				return s.internalError("failed to write expunge notification: %v", err)
			}
		}
	}

	s.Log("[EXPUNGE] command processed, %d messages expunged from DB. Client notified.", len(messagesToExpunge))

	return nil
}
