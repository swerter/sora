package sieveengine

import (
	"context"
	"strings"
	"time"

	"github.com/foxcpp/go-sieve"
	"github.com/foxcpp/go-sieve/interp"
)

type Action string

const (
	ActionKeep     Action = "keep"
	ActionDiscard  Action = "discard"
	ActionFileInto Action = "fileinto"
	ActionRedirect Action = "redirect"
	ActionVacation Action = "vacation"
)

type Result struct {
	Action         Action
	Mailbox        string            // used for fileinto
	RedirectTo     string            // used for redirect
	Flags          []string          // flags to add to the message
	VacationFrom   string            // used for vacation - from address
	VacationSubj   string            // used for vacation - subject
	VacationMsg    string            // used for vacation - message body
	VacationIsMime bool              // used for vacation - is MIME message
	Additional     map[string]string // future-proofing
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

// SieveExecutor implements the Executor interface using the go-sieve library
type SieveExecutor struct {
	script *sieve.Script
	policy *SievePolicy
}

// NewSieveExecutor creates a new SieveExecutor with the given script content
func NewSieveExecutor(scriptContent string) (Executor, error) {
	// Load the script
	scriptReader := strings.NewReader(scriptContent)
	options := sieve.DefaultOptions()
	script, err := sieve.Load(scriptReader, options)
	if err != nil {
		return nil, err
	}

	// Create a policy
	policy := &SievePolicy{}

	return &SieveExecutor{
		script: script,
		policy: policy,
	}, nil
}

// Evaluate evaluates the Sieve script with the given context
func (e *SieveExecutor) Evaluate(ctx Context) (Result, error) {
	// Create envelope and message implementations
	envelope := &SieveEnvelope{
		From: ctx.EnvelopeFrom,
		To:   ctx.EnvelopeTo,
	}

	message := &SieveMessage{
		Headers: ctx.Header,
		Body:    []byte(ctx.Body),
		Size:    len(ctx.Body),
	}

	// Create runtime data
	runtimeCtx := context.Background()
	data := sieve.NewRuntimeData(e.script, e.policy, envelope, message)

	// Execute the script
	err := e.script.Execute(runtimeCtx, data)
	if err != nil {
		return Result{Action: ActionKeep}, err
	}

	// Process the results
	result := Result{
		Action:     ActionKeep,
		Additional: make(map[string]string),
		Flags:      make([]string, 0),
	}

	// Check if vacation response was triggered
	if e.policy.vacationTriggered {
		result.Action = ActionVacation
		result.VacationFrom = e.policy.lastVacationFrom
		result.VacationSubj = e.policy.lastVacationSubj
		result.VacationMsg = e.policy.lastVacationMsg
		result.VacationIsMime = e.policy.lastVacationIsMime

		// Reset the vacation triggered flag for next evaluation
		e.policy.vacationTriggered = false
	}

	// Handle fileinto action
	if len(data.Mailboxes) > 0 {
		// Use the first mailbox (we could support multiple mailboxes in the future)
		result.Action = ActionFileInto
		result.Mailbox = data.Mailboxes[0]
	}

	// Handle redirect action
	if len(data.RedirectAddr) > 0 {
		// Use the first redirect address (we could support multiple redirects in the future)
		result.Action = ActionRedirect
		result.RedirectTo = data.RedirectAddr[0]
	}

	// Handle discard action
	if !data.Keep && !data.ImplicitKeep && len(data.Mailboxes) == 0 && len(data.RedirectAddr) == 0 {
		result.Action = ActionDiscard
	}

	// Handle flags
	if len(data.Flags) > 0 {
		result.Flags = data.Flags
	}

	return result, nil
}

// SievePolicy implements the PolicyReader interface
type SievePolicy struct {
	// Add fields for tracking vacation responses, etc.
	vacationResponses map[string]time.Time
	// Track the last vacation response that was triggered
	lastVacationFrom   string
	lastVacationSubj   string
	lastVacationMsg    string
	lastVacationIsMime bool
	vacationTriggered  bool
}

// RedirectAllowed checks if the redirect action is allowed
func (p *SievePolicy) RedirectAllowed(ctx context.Context, d *interp.RuntimeData, addr string) (bool, error) {
	// Implement your policy for redirects
	// For now, always allow redirects
	return true, nil
}

// VacationResponseAllowed checks if a vacation response is allowed
func (p *SievePolicy) VacationResponseAllowed(ctx context.Context, d *interp.RuntimeData,
	recipient, handle string, duration time.Duration) (bool, error) {
	// Initialize the map if it's nil
	if p.vacationResponses == nil {
		p.vacationResponses = make(map[string]time.Time)
	}

	// Check if we've sent a response to this recipient recently
	lastSent, exists := p.vacationResponses[recipient]
	if exists && time.Since(lastSent) < duration {
		// We've sent a response recently, don't send another one
		return false, nil
	}

	// Update the last sent time
	p.vacationResponses[recipient] = time.Now()

	return true, nil
}

// SendVacationResponse sends a vacation response
func (p *SievePolicy) SendVacationResponse(ctx context.Context, d *interp.RuntimeData,
	recipient, from, subject, body string, isMime bool) error {
	// Store the vacation response details
	p.lastVacationFrom = from
	p.lastVacationSubj = subject
	p.lastVacationMsg = body
	p.lastVacationIsMime = isMime
	p.vacationTriggered = true

	// Implement the actual sending of vacation responses
	// For now, just log the response (this would be replaced with actual email sending)
	return nil
}

// SieveEnvelope implements the Envelope interface
type SieveEnvelope struct {
	From string
	To   string
	Auth string
}

func (e *SieveEnvelope) EnvelopeFrom() string {
	return e.From
}

func (e *SieveEnvelope) EnvelopeTo() string {
	return e.To
}

func (e *SieveEnvelope) AuthUsername() string {
	return e.Auth
}

// SieveMessage implements the Message interface
type SieveMessage struct {
	Headers map[string][]string
	Body    []byte
	Size    int
}

func (m *SieveMessage) HeaderGet(key string) ([]string, error) {
	return m.Headers[key], nil
}

func (m *SieveMessage) MessageSize() int {
	return m.Size
}
