package sieveengine

type Action string

const (
	ActionKeep     Action = "keep"
	ActionDiscard  Action = "discard"
	ActionFileInto Action = "fileinto"
)

type Result struct {
	Action     Action
	Mailbox    string            // used for fileinto
	Additional map[string]string // future-proofing
}

type Context struct {
	EnvelopeFrom string
	EnvelopeTo   string
	Header       map[string][]string
	Body         string
}

type Executor interface {
	Evaluate(ctx Context) (Result, error)
}
