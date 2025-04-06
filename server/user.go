package server

type User struct {
	Address
	userID int64
}

func NewUser(address Address, userID int64) *User {
	return &User{
		Address: address,
		userID:  userID,
	}
}

func (u *User) UserID() int64 {
	return u.userID
}
