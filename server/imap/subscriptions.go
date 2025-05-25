package imap

import (
	"github.com/migadu/sora/consts"
)

// Subscribe to a mailbox
func (s *IMAPSession) Subscribe(mailboxName string) error {
	return s.updateSubscriptionStatus(mailboxName, true)
}

// Unsubscribe from a mailbox
func (s *IMAPSession) Unsubscribe(mailboxName string) error {
	return s.updateSubscriptionStatus(mailboxName, false)
}

// Helper function to handle both subscribe and unsubscribe logic
func (s *IMAPSession) updateSubscriptionStatus(mailboxName string, subscribe bool) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	mailbox, err := s.server.db.GetMailboxByName(s.ctx, s.UserID(), mailboxName)
	if err != nil {
		if err == consts.ErrMailboxNotFound {
			s.Log("Mailbox '%s' does not exist", mailboxName)
			return nil
		}
		return s.internalError("failed to fetch mailbox '%s': %v", mailboxName, err)
	}

	err = s.server.db.SetMailboxSubscribed(s.ctx, mailbox.ID, s.UserID(), subscribe)
	if err != nil {
		return s.internalError("failed to set subscription status for mailbox '%s': %v", mailboxName, err)
	}

	action := "subscribed"
	if !subscribe {
		action = "unsubscribed"
	}
	s.Log("[SUBSCRIBE] mailbox '%s' %s", mailboxName, action)

	return nil
}
