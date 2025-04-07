package imap

import (
	"context"
	"log"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

func (s *IMAPSession) Search(numKind imapserver.NumKind, criteria *imap.SearchCriteria, options *imap.SearchOptions) (*imap.SearchData, error) {
	ctx := context.Background()

	if s.mailbox.numMessages == 0 && len(criteria.SeqNum) > 0 {
		log.Println("Skipping UID SEARCH because mailbox is empty")
		return &imap.SearchData{
			All:   imap.UIDSet{},
			UID:   numKind == imapserver.NumKindUID,
			Count: 0,
		}, nil
	}
	messages, err := s.server.db.GetMessagesWithCriteria(ctx, s.mailbox.ID, numKind, criteria)
	if err != nil {
		return nil, s.internalError("failed to search messages: %v", err)
	}

	var uids imap.UIDSet
	for _, msg := range messages {
		uids.AddNum(msg.UID) // Collect the message UIDs
	}

	searchData := &imap.SearchData{
		All:   uids,
		UID:   numKind == imapserver.NumKindUID, // Set UID flag if searching by UID
		Count: uint32(len(uids)),                // Set the count of matching messages
	}

	searchData.Count = uint32(len(messages))

	return searchData, nil
}
