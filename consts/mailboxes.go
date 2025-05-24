package consts

const MailboxDelimiter = '/'

const MailboxInbox = "INBOX"
const MailboxSent = "Sent"
const MailboxDrafts = "Drafts"
const MailboxArchive = "Archive"
const MailboxJunk = "Junk"
const MailboxTrash = "Trash"

var DefaultMailboxes = []string{
	MailboxInbox,
	MailboxSent,
	MailboxDrafts,
	MailboxArchive,
	MailboxJunk,
	MailboxTrash,
}
