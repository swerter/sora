package imap

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Select(mboxName string, options *imap.SelectOptions) (*imap.SelectData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ctx := context.Background()

	mailbox, err := s.server.db.GetMailboxByName(ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("Mailbox '%s' does not exist", mboxName)
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeNonExistent,
				Text: fmt.Sprintf("mailbox '%s' does not exist", mboxName),
			}
		}

		return nil, s.internalError("failed to fetch mailbox '%s': %v", mboxName, err)
	}

	summary, err := s.server.db.GetMailboxSummary(ctx, mailbox.ID)
	if err != nil {
		return nil, s.internalError("failed to get mailbox summary for '%s': %v", mboxName, err)
	}

	s.mailbox = NewMailbox(mailbox, uint32(summary.NumMessages), summary.HighestModSeq)

	selectData := &imap.SelectData{
		Flags:       s.mailbox.PermittedFlags(),
		NumMessages: s.mailbox.numMessages,
		UIDNext:     imap.UID(summary.UIDNext),
		UIDValidity: mailbox.UIDValidity,
		NumRecent:   uint32(summary.RecentCount),
	}

	s.Log("Mailbox selected: %s", mboxName)
	return selectData, nil
}

func (s *IMAPSession) Unselect() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.mailbox != nil {
		s.Log("Mailbox %s unselected", s.mailbox.Name)
	} else {
		s.Log("Unselect called but no mailbox was selected")
	}
	s.mailbox = nil
	return nil
}
