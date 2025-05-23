package imap

import (
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
)

func (s *IMAPSession) Search(numKind imapserver.NumKind, criteria *imap.SearchCriteria, options *imap.SearchOptions) (*imap.SearchData, error) {
	criteria = s.decodeSearchCriteria(criteria)

	if s.currentNumMessages == 0 && len(criteria.SeqNum) > 0 {
		s.Log("[SEARCH] skipping UID SEARCH because mailbox is empty")
		return &imap.SearchData{
			All:   imap.UIDSet{},
			Count: 0,
		}, nil
	}

	messages, err := s.server.db.GetMessagesWithCriteria(s.ctx, s.selectedMailbox.ID, criteria)
	if err != nil {
		return nil, s.internalError("failed to search messages: %v", err)
	}

	var (
		uids    imap.UIDSet
		seqNums imap.SeqSet
	)
	for _, msg := range messages {
		uids.AddNum(msg.UID)
		seqNums.AddNum(s.sessionTracker.EncodeSeqNum(msg.Seq))
	}

	var all imap.NumSet
	switch numKind {
	case imapserver.NumKindUID:
		all = uids
	case imapserver.NumKindSeq:
		all = seqNums
	}

	searchData := &imap.SearchData{
		All:   all,
		Count: uint32(len(uids)),
	}

	// hasModSeqCriteria := criteria.ModSeq != nil
	// _, hasCondStore := s.server.caps[imap.CapCondStore]

	// if hasCondStore && hasModSeqCriteria {
	// 	var highestModSeq uint64
	// 	for _, msg := range messages {
	// 		var msgModSeq int64
	// 		msgModSeq = msg.CreatedModSeq

	// 		if msg.UpdatedModSeq != nil && *msg.UpdatedModSeq > msgModSeq {
	// 			msgModSeq = *msg.UpdatedModSeq
	// 		}

	// 		if msg.ExpungedModSeq != nil && *msg.ExpungedModSeq > msgModSeq {
	// 			msgModSeq = *msg.ExpungedModSeq
	// 		}

	// 		if uint64(msgModSeq) > highestModSeq {
	// 			highestModSeq = uint64(msgModSeq)
	// 		}
	// 	}

	// 	if highestModSeq > 0 {
	// 		searchData.ModSeq = highestModSeq
	// 	}
	// }

	searchData.Count = uint32(len(messages))

	return searchData, nil
}

func (s *IMAPSession) decodeSearchCriteria(criteria *imap.SearchCriteria) *imap.SearchCriteria {
	decoded := *criteria // make a shallow copy

	decoded.SeqNum = make([]imap.SeqSet, len(criteria.SeqNum))
	for i, seqSet := range criteria.SeqNum {
		decoded.SeqNum[i] = s.decodeNumSet(seqSet).(imap.SeqSet)
	}

	decoded.Not = make([]imap.SearchCriteria, len(criteria.Not))
	for i, not := range criteria.Not {
		decoded.Not[i] = *s.decodeSearchCriteria(&not)
	}
	decoded.Or = make([][2]imap.SearchCriteria, len(criteria.Or))
	for i := range criteria.Or {
		for j := range criteria.Or[i] {
			decoded.Or[i][j] = *s.decodeSearchCriteria(&criteria.Or[i][j])
		}
	}

	return &decoded
}
