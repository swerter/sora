package imap

import (
	"fmt"
	"strings"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapserver"
	"github.com/migadu/sora/consts"
)

func (s *IMAPSession) Select(mboxName string, options *imap.SelectOptions) (*imap.SelectData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var previouslySelectedMailboxID int64
	isReselecting := false

	if s.selectedMailbox != nil {
		previouslySelectedMailboxID = s.selectedMailbox.ID
		s.Log("[SELECT] unselecting mailbox %s (ID: %d) before selecting '%s'.", s.selectedMailbox.Name, previouslySelectedMailboxID, mboxName)

		if strings.EqualFold(s.selectedMailbox.Name, mboxName) {
			isReselecting = true
		}
		s.clearSelectedMailboxState()
	}

	s.Log("[SELECT] attempting to select mailbox: %s reselecting=%v", mboxName, isReselecting)
	mailbox, err := s.server.db.GetMailboxByName(s.ctx, s.UserID(), mboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("[SELECT] mailbox '%s' does not exist", mboxName)
			return nil, &imap.Error{
				Type: imap.StatusResponseTypeNo,
				Code: imap.ResponseCodeNonExistent,
				Text: fmt.Sprintf("mailbox '%s' does not exist", mboxName),
			}
		}

		return nil, s.internalError("failed to fetch mailbox '%s': %v", mboxName, err)
	}

	s.mutex.Unlock()

	currentSummary, err := s.server.db.GetMailboxSummary(s.ctx, mailbox.ID)
	if err != nil {
		s.mutex.Lock()
		return nil, s.internalError("failed to get current summary for selected mailbox '%s': %v", mboxName, err)
	}

	s.mutex.Lock()
	if isReselecting && previouslySelectedMailboxID != 0 && previouslySelectedMailboxID != mailbox.ID {
		s.Log("[SELECT] WARNING: reselecting mailbox '%s', but its ID changed from %d to %d. Proceeding with new ID.", mboxName, previouslySelectedMailboxID, mailbox.ID)
	}
	s.currentNumMessages = uint32(currentSummary.NumMessages)
	s.currentHighestModSeq = currentSummary.HighestModSeq

	s.selectedMailbox = mailbox
	s.mailboxTracker = imapserver.NewMailboxTracker(s.currentNumMessages)
	s.sessionTracker = s.mailboxTracker.NewSession()

	s.Log("[SELECT] mailbox '%s' (ID: %d) selected. Session initial state: NumMessages=%d, HighestModSeqForPolling=%d. Current summary: UIDNext=%d, UIDValidity=%d, ReportedHighestModSeq=%d",
		mboxName, mailbox.ID, s.currentNumMessages, s.currentHighestModSeq, currentSummary.UIDNext, s.selectedMailbox.UIDValidity, currentSummary.HighestModSeq)

	selectData := &imap.SelectData{
		Flags:         s.PermittedFlags(),
		NumMessages:   s.currentNumMessages,
		UIDNext:       imap.UID(currentSummary.UIDNext),
		UIDValidity:   s.selectedMailbox.UIDValidity,
		NumRecent:     uint32(currentSummary.RecentCount),
		HighestModSeq: s.currentHighestModSeq,
	}

	return selectData, nil
}

func (s *IMAPSession) Unselect() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.selectedMailbox != nil {
		s.Log("[SELECT] mailbox %s unselected", s.selectedMailbox.Name)
	}
	s.clearSelectedMailboxState()
	return nil
}
