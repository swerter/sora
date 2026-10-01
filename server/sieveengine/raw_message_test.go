package sieveengine

import (
	"context"
	"strings"
	"testing"
)

func TestMessageBody(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want *string
	}{
		{"CRLF", "Subject: a\r\nFrom: b\r\n\r\nbody\r\n", ptr("body\r\n")},
		{"bare LF", "Subject: a\nFrom: b\n\nbody\n", ptr("body\n")},
		{"mixed endings", "Subject: a\r\nFrom: b\n\r\nbody", ptr("body")},
		{"folded header", "Subject: a\r\n b\r\n\r\nbody", ptr("body")},
		{"blank line inside body is not the separator", "Subject: a\r\n\r\none\r\n\r\ntwo", ptr("one\r\n\r\ntwo")},
		{"empty body", "Subject: a\r\n\r\n", ptr("")},
		{"no header block", "\r\nbody", ptr("body")},
		{"headers only", "Subject: a\r\nFrom: b\r\n", ptr("")},
		{"no line ending", "Subject: a", ptr("")},
		{"empty", "", ptr("")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := messageBody([]byte(tt.msg))
			switch {
			case tt.want == nil && got != nil:
				t.Fatalf("want no body, got %q", got)
			case tt.want != nil && got == nil:
				t.Fatalf("want body %q, got none", *tt.want)
			case tt.want != nil && string(got) != *tt.want:
				t.Fatalf("want body %q, got %q", *tt.want, got)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// TestEvaluateSeesRawMessage checks that the body test walks the MIME structure of
// Context.Message and that the size test measures all of it.
func TestEvaluateSeesRawMessage(t *testing.T) {
	attachment := strings.Repeat("JSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUlJSUl\r\n", 2000)
	msg := "From: sender@example.com\r\nSubject: s\r\nMIME-Version: 1.0\r\n" +
		"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n" +
		"--b\r\nContent-Type: text/plain; charset=utf-8\r\n\r\ninvoice 4711\r\n" +
		"--b\r\nContent-Type: application/pdf\r\nContent-Transfer-Encoding: base64\r\n\r\n" +
		attachment + "--b--\r\n"
	ctx := Context{
		EnvelopeFrom: "sender@example.com",
		EnvelopeTo:   "recipient@example.com",
		Header: map[string][]string{
			"From":         {"sender@example.com"},
			"Subject":      {"s"},
			"Content-Type": {`multipart/mixed; boundary="b"`},
		},
		Message: []byte(msg),
	}

	for _, tt := range []struct {
		script string
		want   Action
	}{
		{`require ["body", "fileinto"]; if body :contains "invoice 4711" { fileinto "Hit"; }`, ActionFileInto},
		{`require ["body", "fileinto"]; if body :content "application/pdf" :contains "" { fileinto "Hit"; }`, ActionFileInto},
		{`require ["fileinto"]; if size :over 100K { fileinto "Hit"; }`, ActionFileInto},
		{`require ["fileinto"]; if size :over 1M { fileinto "Hit"; }`, ActionKeep},
	} {
		executor, err := NewSieveExecutorWithExtensions(tt.script, DefaultSieveExtensions)
		if err != nil {
			t.Fatalf("compile %q: %v", tt.script, err)
		}
		res, err := executor.Evaluate(context.Background(), ctx)
		if err != nil {
			t.Fatalf("evaluate %q: %v", tt.script, err)
		}
		if res.Action != tt.want {
			t.Errorf("%q on a %d-byte multipart message: got %s, want %s", tt.script, len(msg), res.Action, tt.want)
		}
	}
}
