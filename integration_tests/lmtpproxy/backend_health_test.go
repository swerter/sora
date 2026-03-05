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
	dbConfig := common.CreateTestDatabaseConfig(t)
	rdb := common.CreateTestResilientDatabase(t, ctx, dbConfig)
	defer rdb.Close()

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

	// Create LMTP proxy with the unhealthy backend
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create proxy listener: %v", err)
	}
	defer proxyListener.Close()

	proxyServer, err := lmtpproxy.NewServer(lmtpproxy.ServerOptions{
		Name:                     "test-proxy",
		Addr:                     proxyListener.Addr().String(),
		RemoteAddrs:              []string{backendAddr},
		RemoteTLS:                false,
		ConnectTimeout:           500 * time.Millisecond,
		EnableBackendHealthCheck: true, // Enable health checks
		MaxMessageSize:           10 * 1024 * 1024,
	}, rdb, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create LMTP proxy server: %v", err)
	}

	// Start proxy server
	go func() {
		if err := proxyServer.ServeListener(proxyListener); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxyServer.Stop()

	// Wait for backend to be marked unhealthy (3 consecutive failures)
	// Each connection attempt will fail immediately
	time.Sleep(2 * time.Second)

	// Connect to proxy
	client, err := NewLMTPClient(proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer client.Close()

	// Read greeting
	greeting, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read greeting: %v", err)
	}
	if !strings.HasPrefix(greeting, "220") {
		t.Fatalf("Unexpected greeting: %s", greeting)
	}

	// Send LHLO
	if err := client.SendCommand("LHLO test"); err != nil {
		t.Fatalf("Failed to send LHLO: %v", err)
	}
	lhlo, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read LHLO response: %v", err)
	}
	if !strings.HasPrefix(lhlo, "250") {
		t.Fatalf("Unexpected LHLO response: %s", lhlo)
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

	// Send RCPT TO for non-existent user
	// Since all backends are unhealthy and user doesn't exist in proxy DB,
	// we should get TEMPFAIL (450) not permanent reject (550)
	if err := client.SendCommand("RCPT TO:<nonexistent@example.com>"); err != nil {
		t.Fatalf("Failed to send RCPT TO: %v", err)
	}
	rcpt, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read RCPT TO response: %v", err)
	}

	// Verify we get TEMPFAIL (450) not permanent reject (550)
	if strings.HasPrefix(rcpt, "550") {
		t.Errorf("Got permanent reject (550) but expected tempfail (450) when backends are unhealthy.\nResponse: %s", rcpt)
	}
	if !strings.HasPrefix(rcpt, "450") {
		t.Errorf("Expected tempfail (450) when backends are unhealthy, got: %s", rcpt)
	}

	// Verify the response indicates temporary failure
	if !strings.Contains(rcpt, "Requested action not taken") && !strings.Contains(rcpt, "try again") {
		t.Logf("Warning: Response doesn't clearly indicate temporary failure: %s", rcpt)
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
	dbConfig := common.CreateTestDatabaseConfig(t)
	rdb := common.CreateTestResilientDatabase(t, ctx, dbConfig)
	defer rdb.Close()

	// Start a healthy backend (responds to LMTP properly)
	backendAddr := startHealthyLMTPBackend(t)

	// Create LMTP proxy with the healthy backend
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create proxy listener: %v", err)
	}
	defer proxyListener.Close()

	proxyServer, err := lmtpproxy.NewServer(lmtpproxy.ServerOptions{
		Name:                     "test-proxy",
		Addr:                     proxyListener.Addr().String(),
		RemoteAddrs:              []string{backendAddr},
		RemoteTLS:                false,
		ConnectTimeout:           500 * time.Millisecond,
		EnableBackendHealthCheck: true,
		MaxMessageSize:           10 * 1024 * 1024,
	}, rdb, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create LMTP proxy server: %v", err)
	}

	// Start proxy server
	go func() {
		if err := proxyServer.ServeListener(proxyListener); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxyServer.Stop()

	// Wait for backend health check
	time.Sleep(500 * time.Millisecond)

	// Connect to proxy
	client, err := NewLMTPClient(proxyListener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer client.Close()

	// Read greeting
	greeting, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read greeting: %v", err)
	}
	if !strings.HasPrefix(greeting, "220") {
		t.Fatalf("Unexpected greeting: %s", greeting)
	}

	// Send LHLO
	if err := client.SendCommand("LHLO test"); err != nil {
		t.Fatalf("Failed to send LHLO: %v", err)
	}
	lhlo, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read LHLO response: %v", err)
	}
	if !strings.HasPrefix(lhlo, "250") {
		t.Fatalf("Unexpected LHLO response: %s", lhlo)
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

	// Send RCPT TO for non-existent user
	// Since backends are healthy and user doesn't exist,
	// we should get permanent reject (550)
	if err := client.SendCommand("RCPT TO:<nonexistent@example.com>"); err != nil {
		t.Fatalf("Failed to send RCPT TO: %v", err)
	}
	rcpt, err := client.ReadResponse()
	if err != nil {
		t.Fatalf("Failed to read RCPT TO response: %v", err)
	}

	// Verify we get permanent reject (550) when backends are healthy
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
