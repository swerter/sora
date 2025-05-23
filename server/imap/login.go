package imap

import (
	"github.com/emersion/go-imap/v2"
	"github.com/migadu/sora/server"
)

func (s *IMAPSession) Login(address, password string) error {

	addressSt, err := server.NewAddress(address)
	if err != nil {
		s.Log("[LOGIN] failed to parse address: %v", err)
		return &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeAuthenticationFailed,
			Text: "Username not in the correct format",
		}
	}

	s.Log("[LOGIN] authentication attempt for user %s", addressSt.FullAddress())

	userID, err := s.server.db.Authenticate(s.ctx, addressSt.FullAddress(), password)
	if err != nil {
		s.Log("[LOGIN] authentication failed: %v", err)

		return &imap.Error{
			Type: imap.StatusResponseTypeNo,
			Code: imap.ResponseCodeAuthenticationFailed,
			Text: "Invalid username or password",
		}
	}

	// Ensure default mailboxes (INBOX/Drafts/Sent/Spam/Trash) exist
	err = s.server.db.CreateDefaultMailboxes(s.ctx, userID)
	if err != nil {
		return s.internalError("failed to create default mailboxes: %v", err)
	}

	s.IMAPUser = NewIMAPUser(addressSt, userID)
	s.Session.User = &s.IMAPUser.User

	s.Log("[LOGIN] user %s authenticated", address)
	return nil
}
