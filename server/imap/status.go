package imap

import (
	"context"
	"fmt"

	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Status(mboxName string, options *imap.StatusOptions) (*imap.StatusData, error) {
	ctx := context.Background()

	mailbox, err := s.server.db.GetMailboxByName(ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
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

	statusData := &imap.StatusData{
		Mailbox:     mailbox.Name,
		UIDValidity: mailbox.UIDValidity,
	}

	if options.NumMessages {
		num := uint32(summary.NumMessages)
		statusData.NumMessages = &num
	}
	if options.UIDNext {
		statusData.UIDNext = imap.UID(summary.UIDNext)
	}
	if options.NumRecent {
		num := uint32(summary.RecentCount)
		statusData.NumRecent = &num
	}
	if options.NumUnseen {
		num := uint32(summary.UnseenCount)
		statusData.NumUnseen = &num
	}

	return statusData, nil
}
