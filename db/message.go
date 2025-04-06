package db

import (
	"strings"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/google/uuid"
)

// Message struct to represent an email message
type Message struct {
	UserID         int64     // ID of the user who owns the message
	UID            imap.UID  // Unique identifier for the message
	UUID           uuid.UUID // UUID of the message in S3
	MailboxID      int64     // ID of the mailbox the message belongs to
	S3Uploaded     bool      // Indicates if the message is uploaded to S3
	Seq            uint32    // Sequence number of the message in the mailbox
	BitwiseFlags   int       // Bitwise flags for the message (e.g., \Seen, \Flagged)
	FlagsChangedAt time.Time // Time when the flags were last changed
	Subject        string    // Subject of the message
	InternalDate   time.Time // The internal date the message was received
	SentDate       time.Time // The date the message was sent
	Size           int       // Size of the message in bytes
	MessageID      string    // Unique Message-ID from the message headers
	TextBody       string    // Text body of the message
	BodyStructure  imap.BodyStructure
}

// MessagePart represents a part of an email message (e.g., body, attachments)
type MessagePart struct {
	MessageID  int64  // Reference to the message ID
	PartNumber int    // Part number (e.g., 1 for body, 2 for attachments)
	Size       int    // Size of the part in bytes
	S3Key      string // S3 key to reference the part's storage location
	Type       string // MIME type of the part (e.g., "text/plain", "text/html", "application/pdf")
}

// IMAP message flags as bitwise constants
const (
	FlagSeen     = 1 << iota // 1: 000001
	FlagAnswered             // 2: 000010
	FlagFlagged              // 4: 000100
	FlagDeleted              // 8: 001000
	FlagDraft                // 16: 010000
	FlagRecent               // 32: 100000
)

func ContainsFlag(flags int, flag int) bool {
	return flags&flag != 0
}

func FlagToBitwise(flag imap.Flag) int {
	switch strings.ToLower(string(flag)) {
	case "\\seen":
		return FlagSeen
	case "\\answered":
		return FlagAnswered
	case "\\flagged":
		return FlagFlagged
	case "\\deleted":
		return FlagDeleted
	case "\\draft":
		return FlagDraft
	case "\\recent":
		return FlagRecent
	}

	return 0
}

// Convert IMAP flags (e.g., "\Seen", "\Answered") to bitwise flags
func FlagsToBitwise(flags []imap.Flag) int {
	var bitwiseFlags int

	for _, flag := range flags {
		bitwiseFlags |= FlagToBitwise(flag)
	}
	return bitwiseFlags
}

// Convert bitwise flags to IMAP flag strings
func BitwiseToFlags(bitwiseFlags int) []imap.Flag {
	var flags []imap.Flag

	if bitwiseFlags&FlagSeen != 0 {
		flags = append(flags, imap.FlagSeen)
	}
	if bitwiseFlags&FlagAnswered != 0 {
		flags = append(flags, imap.FlagAnswered)
	}
	if bitwiseFlags&FlagFlagged != 0 {
		flags = append(flags, imap.FlagFlagged)
	}
	if bitwiseFlags&FlagDeleted != 0 {
		flags = append(flags, imap.FlagDeleted)
	}
	if bitwiseFlags&FlagDraft != 0 {
		flags = append(flags, imap.FlagDraft)
	}
	// if bitwiseFlags&FlagRecent != 0 {
	// 	flags = append(flags, imap.FlagRecent)
	// }

	return flags
}
