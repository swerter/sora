//go:build integration
// +build integration

package imap_slowloris_test

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
)

// The slowloris check (server/sora_conn.go) disconnects a session after two
// consecutive one-minute windows in which the client sent bytes but the
// server never produced a response, i.e. no command completed. It starts two
// minutes after the handshake. It deliberately does NOT measure volume alone:
// a client that completes a tiny command every few seconds (Alpine's NOOP
// poll, any client without IDLE) is healthy however few bytes it moves, and
// silence is the idle timeout's business.

// loginAndPassGrace connects, logs in, and keeps the session busy until the
// post-handshake grace period is over. It returns the connection, its reader,
// the session start time and the next free command number.
func loginAndPassGrace(t *testing.T, server *common.TestServer, account common.TestAccount, selectInbox bool) (net.Conn, *bufio.Reader, time.Time, int) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", server.Address, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	reader := bufio.NewReader(conn)

	greeting, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read greeting: %v", err)
	}
	if !strings.HasPrefix(greeting, "* OK") {
		t.Fatalf("Invalid greeting: %s", greeting)
	}
	sessionStart := time.Now()

	fmt.Fprintf(conn, "a001 LOGIN %s %s\r\n", account.Email, account.Password)
	loginResp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to authenticate: %v", err)
	}
	if !strings.HasPrefix(loginResp, "a001 OK") {
		t.Fatalf("LOGIN failed: %s", loginResp)
	}
	commandNum := 2

	if selectInbox {
		fmt.Fprintf(conn, "a002 SELECT INBOX\r\n")
		for {
			resp, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("Failed to SELECT INBOX: %v", err)
			}
			if strings.HasPrefix(resp, "a002 OK") {
				break
			}
		}
		commandNum = 3
	}

	t.Logf("--- Grace period: steady NOOPs for 2 minutes ---")
	gracePeriodEnd := sessionStart.Add(2*time.Minute + 5*time.Second)
	ticker := time.NewTicker(1200 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(gracePeriodEnd) {
		<-ticker.C
		commandNum = noop(t, conn, reader, commandNum, "grace period")
	}
	t.Logf("✓ Grace period over at T+%.1fs", time.Since(sessionStart).Seconds())
	return conn, reader, sessionStart, commandNum
}

// noop completes one NOOP round trip and fails the test if the session is gone.
func noop(t *testing.T, conn net.Conn, reader *bufio.Reader, commandNum int, phase string) int {
	t.Helper()
	tag := fmt.Sprintf("a%03d", commandNum)
	fmt.Fprintf(conn, "%s NOOP\r\n", tag)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("❌ Disconnected during %s: %v", phase, err)
	}
	if strings.HasPrefix(resp, "* BYE") {
		t.Fatalf("❌ Server sent BYE during %s: %s", phase, strings.TrimSpace(resp))
	}
	if !strings.HasPrefix(resp, tag+" OK") {
		t.Fatalf("NOOP failed during %s: %s", phase, resp)
	}
	return commandNum + 1
}

// pollLikeAlpine completes a NOOP every 15 seconds for the given duration:
// four round trips a minute, roughly 170 bytes/min, far under the 512
// bytes/min threshold. The session must survive every measurement window.
func pollLikeAlpine(t *testing.T, conn net.Conn, reader *bufio.Reader, commandNum int, d time.Duration) int {
	t.Helper()
	t.Logf("--- Alpine-style poll: one NOOP every 15s for %.0fs (must survive) ---", d.Seconds())
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		time.Sleep(15 * time.Second)
		commandNum = noop(t, conn, reader, commandNum, "Alpine-style NOOP poll")
	}
	t.Logf("✅ Survived %.0fs of low-volume polling with completed commands", d.Seconds())
	return commandNum
}

// trickleUntilClosed feeds the server one byte of a command line every 20
// seconds without ever sending the CRLF that would complete it: the actual
// slowloris signature. It returns the BYE line the server sent (empty if the
// connection was simply closed) and fails the test if the server has not
// closed the session by the deadline.
//
// On IMAP the library's 30-second command read deadline (go-imap
// imapserver cmdReadTimeout) cuts a trickled command before the throughput
// guard's two one-minute windows elapse, so the close here normally arrives
// as a plain EOF within a minute. The guard's own rule is pinned by the unit
// tests in server/sora_conn_throughput_test.go; this phase proves the server
// as a whole still refuses to hold a command that never completes.
func trickleUntilClosed(t *testing.T, conn net.Conn, reader *bufio.Reader, sessionStart time.Time, limit time.Duration) string {
	t.Helper()
	t.Logf("--- Slowloris: trickling a command that never completes (must be disconnected within %.0fs) ---", limit.Seconds())

	type line struct {
		text string
		err  error
	}
	lines := make(chan line, 16)
	go func() {
		for {
			text, err := reader.ReadString('\n')
			lines <- line{text, err}
			if err != nil {
				return
			}
		}
	}()

	deadline := time.After(limit)
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	fmt.Fprint(conn, "a") // first fragment, never completed
	for {
		select {
		case l := <-lines:
			if l.err != nil {
				t.Logf("✅ Server closed the stalled session at T+%.1fs (%v)", time.Since(sessionStart).Seconds(), l.err)
				return ""
			}
			if strings.HasPrefix(l.text, "* BYE") {
				t.Logf("✅ Server sent BYE at T+%.1fs: %s", time.Since(sessionStart).Seconds(), strings.TrimSpace(l.text))
				return l.text
			}
			t.Logf("   (ignoring untagged line while stalled: %s)", strings.TrimSpace(l.text))
		case <-ticker.C:
			if _, err := fmt.Fprint(conn, "a"); err != nil {
				t.Logf("✅ Write failed, server closed the stalled session at T+%.1fs (%v)", time.Since(sessionStart).Seconds(), err)
				return ""
			}
		case <-deadline:
			t.Fatalf("❌ FAILED: stalled session still open after %.0fs; slowloris protection is not working", limit.Seconds())
		}
	}
}

// TestSlowlorisProtection: a low-volume poller that completes commands is
// never cut, while a client that never completes a command is.
func TestSlowlorisProtection(t *testing.T) {
	common.SkipIfDatabaseUnavailable(t)
	if testing.Short() {
		t.Skip("Skipping long-running slowloris test in short mode")
	}

	// 2-minute idle timeout: the trickle sends a byte every 20s, so only the
	// throughput check can end the session. 512 bytes/min threshold.
	server, account := common.SetupIMAPServerWithSlowloris(t, 2*time.Minute, 512)

	conn, reader, sessionStart, commandNum := loginAndPassGrace(t, server, account, false)

	// Three full measurement windows: the old volume-only rule cut this
	// session in the second one.
	pollLikeAlpine(t, conn, reader, commandNum, 3*time.Minute+10*time.Second)

	// Two stalled windows plus slack for window alignment.
	bye := trickleUntilClosed(t, conn, reader, sessionStart, 3*time.Minute+30*time.Second)
	if bye != "" && !strings.Contains(bye, "Connection too slow") {
		t.Errorf("expected the slow-connection BYE, got: %s", strings.TrimSpace(bye))
	}
}

// TestSlowlorisIdleSuspension verifies that IDLE suspends the check (client
// silence is expected there) and that it resumes after DONE.
func TestSlowlorisIdleSuspension(t *testing.T) {
	common.SkipIfDatabaseUnavailable(t)
	if testing.Short() {
		t.Skip("Skipping long-running slowloris test in short mode")
	}

	// 10-minute idle timeout (longer than the test), 512 bytes/min threshold.
	server, account := common.SetupIMAPServerWithSlowloris(t, 10*time.Minute, 512)

	conn, reader, sessionStart, commandNum := loginAndPassGrace(t, server, account, true)

	t.Logf("--- IDLE for 3+ minutes (must survive) ---")
	tag := fmt.Sprintf("a%03d", commandNum)
	fmt.Fprintf(conn, "%s IDLE\r\n", tag)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to enter IDLE: %v", err)
	}
	if !strings.HasPrefix(resp, "+ idling") {
		t.Fatalf("IDLE not accepted: %s", resp)
	}
	t.Logf("✓ Entered IDLE at T+%.1fs", time.Since(sessionStart).Seconds())

	// Read until the deadline expires. The server sends periodic untagged
	// "* OK Still here" keepalives during IDLE, so reaching the deadline is the
	// success condition and any other read error means it closed the connection.
	idleDuration := 3*time.Minute + 10*time.Second
	conn.SetReadDeadline(time.Now().Add(idleDuration))
	for {
		if _, err := reader.ReadString('\n'); err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				break
			}
			t.Fatalf("❌ FAILED: Disconnected during IDLE at T+%.1fs: %v", time.Since(sessionStart).Seconds(), err)
		}
	}
	t.Logf("✅ Stayed in IDLE for %.0fs", idleDuration.Seconds())

	fmt.Fprintf(conn, "DONE\r\n")
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	// Drain any keepalives still queued ahead of the tagged completion.
	for {
		resp, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to exit IDLE: %v", err)
		}
		if strings.HasPrefix(resp, tag+" ") {
			break
		}
	}
	if !strings.Contains(resp, "OK") || !strings.Contains(resp, "IDLE") {
		t.Fatalf("IDLE exit failed: %s", resp)
	}
	conn.SetReadDeadline(time.Time{})
	t.Logf("✓ Exited IDLE at T+%.1fs", time.Since(sessionStart).Seconds())

	// The check is active again: a command that never completes must be cut.
	// ResumeThroughputChecking opened a fresh window at DONE, so two stalled
	// windows fit well inside the limit.
	trickleUntilClosed(t, conn, reader, sessionStart, 3*time.Minute+30*time.Second)
}
