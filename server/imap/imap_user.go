package imap

import (
	"sync"

	"github.com/migadu/sora/server"
)

type IMAPUser struct {
	server.User
	mutex sync.Mutex
}

func NewIMAPUser(address server.Address, userID int64) *IMAPUser {
	return &IMAPUser{
		User: *server.NewUser(address, userID),
	}
}

func (u *IMAPUser) UserID() int64 {
	return u.User.UserID()
}

func (u *IMAPUser) Domain() string {
	return u.User.Domain()
}

func (u *IMAPUser) LocalPart() string {
	return u.User.LocalPart()
}

func (u *IMAPUser) FullAddress() string {
	return u.User.FullAddress()
}
