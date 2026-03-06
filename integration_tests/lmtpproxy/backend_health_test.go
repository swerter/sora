//go:build integration

package lmtpproxy_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
	"github.com/migadu/sora/server/lmtpproxy"
)

// TestLMTPProxy_UserNotFound_AllBackendsUnhealthy tests that when:
// 1. User doesn't exist in proxy's database (successful database query, but user not found)
// 2. All backends are unhealthy (down/unreachable)
// The proxy returns TEMPFAIL (450) instead of permanent reject (550)
//
// This prevents wrongly bouncing messages in scenarios like:
//   - Proxy has separate/lagging database instance from backends
//   - User exists on backends but not yet replicated to proxy database
//   - Backends temporarily unavailable, can't verify user existence
//
// Real-world scenario from production:
//   - Backend database went down
//   - Proxy's local database was queried and returned "user not found"
//   - Without this check, messages were permanently rejected (550)
//   - With this check, messages get tempfail (450) allowing retry when backends recover
func TestLMTPProxy_UserNotFound_AllBackendsUnhealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a database for the proxy (user does NOT exist in this database)
	// This simulates a proxy with separate/incomplete database from backends
	rdb := common.SetupTestDatabase(t)

	// Create a fake backend that accepts connections but then closes immediately
	// This simulates an unhealthy backend (connection failures)
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create backend listener: %v", err)
	}
	backendAddr := backendListener.Addr().String()

	// Accept connections and immediately close them (simulates unhealthy backend)
	// This will cause the connection manager to mark the backend as unhealthy
	// after 3 consecutive connection failures
	go func() {
		for {
			conn, err := backendListener.Accept()
			if err != nil {
				return // Listener closed
			}
			conn.Close() // Close immediately without responding
		}
	}()
	defer backendListener.Close()

	// Create LMTP proxy with the unhealthy backend.
	// Use "127.0.0.1:0" so the OS assigns a free port; retrieve it via Addr() after Start().
	proxyServer, err := lmtpproxy.New(ctx, rdb, "test-proxy", lmtpproxy.ServerOptions{
		Name:                     "test-proxy",
		Addr:                     "127.0.0.1:0",
		RemoteAddrs:              []string{backendAddr},
		RemoteTLS:                false,
		ConnectTimeout:           500 * time.Millisecond,
		EnableBackendHealthCheck: true, // Enable health checks
		MaxMessageSize:           10 * 1024 * 1024,
		TrustedProxies:           []string{"127.0.0.0/8", "::1/128"}, // Trust localhost connections
		AuthIdleTimeout:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create LMTP proxy server: %v", err)
	}

	// Start proxy server
	go func() {
		if err := proxyServer.Start(); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxyServer.Stop()

	// Wait for server to bind its listener, then retrieve the actual address.
	time.Sleep(100 * time.Millisecond)
	proxyAddress := proxyServer.Addr()

	// The connection manager only marks a backend unhealthy after RecordConnectionFailure
	// is called 3 times. There is no background health-probe goroutine — health is updated
	// reactively when sessions try to connect. Simulate the 3 failures directly so the
	// backend is definitively unhealthy before the RCPT TO check.
	cm := proxyServer.GetConnectionManager()
	cm.RecordConnectionFailure(backendAddr)
	cm.RecordConnectionFailure(backendAddr)
	cm.RecordConnectionFailure(backendAddr)

	// Connect to proxy (NewLMTPClient already reads and validates the 220 greeting).
	client, err := NewLMTPClient(proxyAddress)
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer client.Close()

	// Send LHLO — response is multi-line so drain all capability lines.
	if err := client.SendCommand("LHLO test"); err != nil {
		t.Fatalf("Failed to send LHLO: %v", err)
	}
	lhlo, err := client.ReadMultilineResponse()
	if err != nil {
		t.Fatalf("Failed to read LHLO response: %v", err)
	}
	if !strings.HasPrefix(lhlo[0], "250") {
		t.Fatalf("Unexpected LHLO response: %v", lhlo)
	}

	// Send MAIL FROM
	if err := client.SendCommand("MAIL FROM:<sender@example.com>"); err != nil {
		t.Fatalf("Failed to send MAIL FROM: %v", err)
	}
	mail, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read MAIL FROM response: %v", err)
	}
	if !strings.HasPrefix(mail, "250") {
		t.Fatalf("Unexpected MAIL FROM response: %s", mail)
	}

	// Send RCPT TO for non-existent user.
	// Since all backends are unhealthy and user doesn't exist in proxy DB,
	// we should get TEMPFAIL (450) not permanent reject (550).
	if err := client.SendCommand("RCPT TO:<nonexistent@example.com>"); err != nil {
		t.Fatalf("Failed to send RCPT TO: %v", err)
	}
	rcpt, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read RCPT TO response: %v", err)
	}

	// Verify we get TEMPFAIL (450) not permanent reject (550).
	if strings.HasPrefix(rcpt, "550") {
		t.Errorf("Got permanent reject (550) but expected tempfail (450) when backends are unhealthy.\nResponse: %s", rcpt)
	}
	if !strings.HasPrefix(rcpt, "450") {
		t.Errorf("Expected tempfail (450) when backends are unhealthy, got: %s", rcpt)
	}
}

// TestLMTPProxy_UserNotFound_BackendsHealthy tests that when:
// 1. User doesn't exist in proxy's database
// 2. At least one backend is healthy
// The proxy returns permanent reject (550) as expected
func TestLMTPProxy_UserNotFound_BackendsHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a database for the proxy (user does NOT exist in this database)
	rdb := common.SetupTestDatabase(t)

	// Start a healthy backend (responds to LMTP properly)
	backendAddr := startHealthyLMTPBackend(t)

	// Create LMTP proxy with the healthy backend.
	// Use "127.0.0.1:0" so the OS assigns a free port; retrieve it via Addr() after Start().
	proxyServer, err := lmtpproxy.New(ctx, rdb, "test-proxy", lmtpproxy.ServerOptions{
		Name:                     "test-proxy",
		Addr:                     "127.0.0.1:0",
		RemoteAddrs:              []string{backendAddr},
		RemoteTLS:                false,
		ConnectTimeout:           500 * time.Millisecond,
		EnableBackendHealthCheck: true,
		MaxMessageSize:           10 * 1024 * 1024,
		TrustedProxies:           []string{"127.0.0.0/8", "::1/128"}, // Trust localhost connections
		AuthIdleTimeout:          5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create LMTP proxy server: %v", err)
	}

	// Start proxy server
	go func() {
		if err := proxyServer.Start(); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxyServer.Stop()

	// Wait for server to bind its listener, then retrieve the actual address.
	time.Sleep(100 * time.Millisecond)
	proxyAddress := proxyServer.Addr()

	// Connect to proxy (NewLMTPClient already reads and validates the 220 greeting).
	client, err := NewLMTPClient(proxyAddress)
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer client.Close()

	// Send LHLO — response is multi-line so drain all capability lines.
	if err := client.SendCommand("LHLO test"); err != nil {
		t.Fatalf("Failed to send LHLO: %v", err)
	}
	lhlo, err := client.ReadMultilineResponse()
	if err != nil {
		t.Fatalf("Failed to read LHLO response: %v", err)
	}
	if !strings.HasPrefix(lhlo[0], "250") {
		t.Fatalf("Unexpected LHLO response: %v", lhlo)
	}

	// Send MAIL FROM
	if err := client.SendCommand("MAIL FROM:<sender@example.com>"); err != nil {
		t.Fatalf("Failed to send MAIL FROM: %v", err)
	}
	mail, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read MAIL FROM response: %v", err)
	}
	if !strings.HasPrefix(mail, "250") {
		t.Fatalf("Unexpected MAIL FROM response: %s", mail)
	}

	// Send RCPT TO for non-existent user.
	// Since backends are healthy and user doesn't exist,
	// we should get permanent reject (550).
	if err := client.SendCommand("RCPT TO:<nonexistent@example.com>"); err != nil {
		t.Fatalf("Failed to send RCPT TO: %v", err)
	}
	rcpt, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read RCPT TO response: %v", err)
	}

	// Verify we get permanent reject (550) when backends are healthy.
	if !strings.HasPrefix(rcpt, "550") {
		t.Errorf("Expected permanent reject (550) when backends are healthy, got: %s", rcpt)
	}
}

// startHealthyLMTPBackend starts a simple LMTP server that responds to basic commands
func startHealthyLMTPBackend(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create backend listener: %v", err)
	}

	// Simple LMTP backend that responds to basic commands
	go func() {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			// Handle connection
			go func(c net.Conn) {
				defer c.Close()

				// Send greeting
				fmt.Fprintf(c, "220 test-backend LMTP ready\r\n")

				// Read and respond to commands
				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}

					cmd := strings.ToUpper(strings.TrimSpace(line))
					if strings.HasPrefix(cmd, "LHLO") {
						fmt.Fprintf(c, "250-test-backend\r\n")
						fmt.Fprintf(c, "250 8BITMIME\r\n")
					} else if strings.HasPrefix(cmd, "MAIL FROM") {
						fmt.Fprintf(c, "250 OK\r\n")
					} else if strings.HasPrefix(cmd, "RCPT TO") {
						fmt.Fprintf(c, "250 OK\r\n")
					} else if strings.HasPrefix(cmd, "DATA") {
						fmt.Fprintf(c, "354 Start mail input\r\n")
					} else if strings.HasPrefix(cmd, "QUIT") {
						fmt.Fprintf(c, "221 Bye\r\n")
						return
					} else if cmd == "." {
						fmt.Fprintf(c, "250 OK\r\n")
					}
				}
			}(conn)
		}
	}()

	return listener.Addr().String()
}
