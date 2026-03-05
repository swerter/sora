//go:build integration

package lmtpproxy_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
	"github.com/migadu/sora/server/lmtpproxy"
)

// TestLMTPProxy_InvalidRecipientAddress verifies that syntactically invalid RCPT TO
// addresses are rejected with 550 5.1.3 (permanent failure), NOT 451 (temporary failure).
//
// Regression test for: "undisclosed-recipients:"@migadu.com being answered with
// "451 4.3.0 Temporary failure" instead of a permanent 550.
func TestLMTPProxy_InvalidRecipientAddress(t *testing.T) {
	common.SkipIfDatabaseUnavailable(t)

	// The proxy only needs a backend address configured; since the address validation
	// happens before any backend connection is attempted, the backend does not need
	// to be reachable for these test cases.
	rdb := common.SetupTestDatabase(t)
	proxyAddr := common.GetRandomAddress(t)

	server, err := lmtpproxy.New(
		context.Background(),
		rdb,
		"localhost",
		lmtpproxy.ServerOptions{
			Name:            "test-proxy-invalid-addr",
			Addr:            proxyAddr,
			RemoteAddrs:     []string{"127.0.0.1:19999"}, // unreachable – intentional
			TrustedProxies:  []string{"127.0.0.0/8", "::1/128"},
			AuthIdleTimeout: 5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}
	go server.Start()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	cases := []struct {
		name     string
		rcptTo   string // raw value placed between < >
		wantCode string // expected SMTP response code prefix
		wantPerm bool   // true = permanent (5xx), false = temporary (4xx)
	}{
		{
			name:     "undisclosed_recipients_with_quotes",
			rcptTo:   `"undisclosed-recipients:"@migadu.com`,
			wantCode: "550",
			wantPerm: true,
		},
		{
			name:     "colon_in_local_part",
			rcptTo:   "foo:bar@example.com",
			wantCode: "550",
			wantPerm: true,
		},
		{
			// A space splits the RCPT TO token before reaching handleRecipient;
			// extractAddress gets "<foo" (no closing ">") and returns "".
			// The proxy already rejects this with 501 at the parsing layer.
			name:     "space_in_local_part",
			rcptTo:   "foo bar@example.com",
			wantCode: "501",
			wantPerm: true,
		},
		{
			name:     "single_label_domain_no_dot",
			rcptTo:   "user@localhostonly",
			wantCode: "550",
			wantPerm: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewLMTPClient(proxyAddr)
			if err != nil {
				t.Fatalf("Failed to connect to proxy: %v", err)
			}
			defer client.Close()

			// LHLO
			if err := client.SendCommand("LHLO localhost"); err != nil {
				t.Fatalf("Failed to send LHLO: %v", err)
			}
			if _, err := client.ReadMultilineResponse(); err != nil {
				t.Fatalf("Failed to read LHLO response: %v", err)
			}

			// MAIL FROM
			if err := client.SendCommand("MAIL FROM:<sender@example.com>"); err != nil {
				t.Fatalf("Failed to send MAIL FROM: %v", err)
			}
			resp, err := client.ReadResponse()
			if err != nil {
				t.Fatalf("Failed to read MAIL FROM response: %v", err)
			}
			if !strings.HasPrefix(resp, "250") {
				t.Fatalf("MAIL FROM failed: %s", resp)
			}

			// RCPT TO with the invalid address
			if err := client.SendCommand("RCPT TO:<" + tc.rcptTo + ">"); err != nil {
				t.Fatalf("Failed to send RCPT TO: %v", err)
			}
			resp, err = client.ReadResponse()
			if err != nil {
				t.Fatalf("Failed to read RCPT TO response: %v", err)
			}

			if !strings.HasPrefix(resp, tc.wantCode) {
				t.Errorf("recipient %q: expected %s response, got: %s", tc.rcptTo, tc.wantCode, resp)
			}
			if tc.wantPerm && strings.HasPrefix(resp, "4") {
				t.Errorf("recipient %q: got temporary failure %s but expected permanent 5xx", tc.rcptTo, resp)
			}
			if !tc.wantPerm && strings.HasPrefix(resp, "5") {
				t.Errorf("recipient %q: got permanent failure %s but expected temporary 4xx", tc.rcptTo, resp)
			}
			t.Logf("✓ %s → %s", tc.rcptTo, resp)
		})
	}
}
