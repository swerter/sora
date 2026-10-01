//go:build integration

package httpapi

import (
	"fmt"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
)

// TestAdminAPI_DeliverMail_SieveBodyAndSizeSeeTheStoredMessage is the Admin API half of
// lmtp's TestLMTP_SieveBodyAndSizeSeeTheStoredMessage, over the same cases: the Sieve
// body and size tests must evaluate the message as stored, not its extracted search text,
// on every ingress path.
func TestAdminAPI_DeliverMail_SieveBodyAndSizeSeeTheStoredMessage(t *testing.T) {
	common.SkipIfDatabaseUnavailable(t)

	server, _ := setupHTTPAPIServerWithUploader(t)
	defer server.Close()

	for i, tc := range common.SieveRawMessageCases() {
		t.Run(tc.Name, func(t *testing.T) {
			marker := fmt.Sprintf("raw%d-%d", i, time.Now().UnixNano())
			got := deliverMessageWithScript(t, server, tc.Script, marker, func(email string) string {
				return tc.Message(email, marker)
			})
			if got != tc.Mailbox {
				t.Errorf("script %q: want the message in %s, got %s", tc.Script, tc.Mailbox, got)
			}
		})
	}
}
