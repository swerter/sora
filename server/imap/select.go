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

	s.Log("Selecting mailbox: %s", mboxName)

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

	// Verify the mailbox exists and has messages
	summary, err := s.server.db.GetMailboxSummary(ctx, mailbox.ID)
	if err != nil {
		return nil, s.internalError("failed to get mailbox summary for '%s': %v", mboxName, err)
	}

	// Double-check message count with a direct query
	count, _, err := s.server.db.GetMailboxMessageCountAndSizeSum(ctx, mailbox.ID)
	if err != nil {
		s.Log("Warning: Failed to get message count for mailbox '%s': %v", mboxName, err)
	} else if count != summary.NumMessages {
		s.Log("Warning: Message count mismatch for mailbox '%s': summary=%d, direct count=%d",
			mboxName, summary.NumMessages, count)
		// Use the direct count as it's more reliable
		summary.NumMessages = count
	}

	s.Log("Mailbox '%s' has %d messages, UIDNext=%d, HighestModSeq=%d",
		mboxName, summary.NumMessages, summary.UIDNext, summary.HighestModSeq)

	s.mailbox = NewMailbox(mailbox, uint32(summary.NumMessages), summary.HighestModSeq)

	selectData := &imap.SelectData{
		Flags:       s.mailbox.PermittedFlags(),
		NumMessages: s.mailbox.numMessages,
		UIDNext:     imap.UID(summary.UIDNext),
		UIDValidity: mailbox.UIDValidity,
		NumRecent:   uint32(summary.RecentCount),
	}

	s.Log("Mailbox selected: %s (NumMessages=%d, UIDNext=%d, UIDValidity=%d)",
		mboxName, selectData.NumMessages, selectData.UIDNext, selectData.UIDValidity)

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
