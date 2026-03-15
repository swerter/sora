//go:build integration

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/migadu/sora/server"
)

// TestCrossProtocolAffinityPortMismatch demonstrates that cross-protocol affinity
// fails when protocol proxies use different ports for the same backend host.
//
// Real-world config:
//
//	IMAP proxy: remote_addrs=["backend1:143","backend2:143"]
//	POP3 proxy: remote_addrs=["backend1:110","backend2:110"]
//
// When IMAP sets affinity "user→backend1:143" and POP3 does cross-protocol lookup,
// it gets "backend1:143" — but POP3's pool only has "backend1:110".
// IsBackendHealthy("backend1:143") returns false (not in pool), which:
//  1. Fails to route via cross-protocol affinity
//  2. Incorrectly deletes the IMAP affinity
func TestCrossProtocolAffinityPortMismatch(t *testing.T) {
	// Create cluster + affinity manager
	cluster1, err := createTestCluster("cross-proto-port-1", 26946, []string{})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster1.Shutdown()

	affinity := server.NewAffinityManager(cluster1, true, 24*time.Hour, 1*time.Hour)
	defer affinity.Stop()

	// Simulate IMAP proxy setting affinity (backend1 at IMAP port)
	affinity.SetBackend("user@example.com", "backend1:143", "imap")

	// Create POP3 proxy's connection manager (same hosts, different port)
	pop3Backends := []string{"backend1:110", "backend2:110"}
	pop3ConnMgr, err := NewConnectionManager(pop3Backends, 110, false, false, false, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create POP3 connection manager: %v", err)
	}
	pop3ConnMgr.SetAffinityManager(affinity)

	// Verify IMAP affinity exists before routing
	imapBackend, imapFound := affinity.GetBackend("user@example.com", "imap")
	if !imapFound || imapBackend != "backend1:143" {
		t.Fatalf("IMAP affinity should exist: found=%v backend=%s", imapFound, imapBackend)
	}

	// DetermineRoute for POP3 — should use cross-protocol affinity from IMAP
	result, err := DetermineRoute(RouteParams{
		Ctx:            context.Background(),
		Username:       "user@example.com",
		Protocol:       "pop3",
		ConnManager:    pop3ConnMgr,
		EnableAffinity: true,
		ProxyName:      "POP3 Proxy",
	})
	if err != nil {
		t.Fatalf("DetermineRoute failed: %v", err)
	}

	// Cross-protocol affinity should route POP3 to backend1 (at POP3's port)
	if result.PreferredAddr != "backend1:110" {
		t.Errorf("Expected cross-protocol affinity to route to 'backend1:110', got '%s' (method: %s)",
			result.PreferredAddr, result.RoutingMethod)
	}
	if result.RoutingMethod != "affinity_cross_protocol" {
		t.Errorf("Expected routing method 'affinity_cross_protocol', got '%s'", result.RoutingMethod)
	}

	// IMAP affinity must NOT be deleted (the IMAP backend is healthy, POP3 proxy just doesn't know its port)
	imapBackend, imapFound = affinity.GetBackend("user@example.com", "imap")
	if !imapFound {
		t.Error("IMAP affinity was incorrectly deleted by POP3 proxy's cross-protocol lookup")
	} else if imapBackend != "backend1:143" {
		t.Errorf("IMAP affinity changed unexpectedly: %s", imapBackend)
	}

	t.Logf("✅ Cross-protocol affinity correctly resolved backend1:143 (IMAP) → backend1:110 (POP3)")
}

// TestCrossProtocolAffinityHostNotInPool verifies that cross-protocol affinity
// is safely skipped when the backend host is not in the requesting proxy's pool.
func TestCrossProtocolAffinityHostNotInPool(t *testing.T) {
	cluster1, err := createTestCluster("cross-proto-nohost-1", 26947, []string{})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster1.Shutdown()

	affinity := server.NewAffinityManager(cluster1, true, 24*time.Hour, 1*time.Hour)
	defer affinity.Stop()

	// IMAP has affinity to backend3, which POP3 proxy doesn't know about
	affinity.SetBackend("user@example.com", "backend3:143", "imap")

	// POP3 proxy only knows backend1 and backend2
	pop3Backends := []string{"backend1:110", "backend2:110"}
	pop3ConnMgr, err := NewConnectionManager(pop3Backends, 110, false, false, false, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create connection manager: %v", err)
	}
	pop3ConnMgr.SetAffinityManager(affinity)

	result, err := DetermineRoute(RouteParams{
		Ctx:            context.Background(),
		Username:       "user@example.com",
		Protocol:       "pop3",
		ConnManager:    pop3ConnMgr,
		EnableAffinity: true,
		ProxyName:      "POP3 Proxy",
	})
	if err != nil {
		t.Fatalf("DetermineRoute failed: %v", err)
	}

	// Should NOT use cross-protocol affinity (host not in pool) — fall through to consistent hash
	if result.RoutingMethod == "affinity_cross_protocol" {
		t.Errorf("Should not use cross-protocol affinity when host is not in pool, got method: %s addr: %s",
			result.RoutingMethod, result.PreferredAddr)
	}

	// IMAP affinity must NOT be deleted
	imapBackend, imapFound := affinity.GetBackend("user@example.com", "imap")
	if !imapFound {
		t.Error("IMAP affinity was incorrectly deleted when host wasn't in POP3 pool")
	} else if imapBackend != "backend3:143" {
		t.Errorf("IMAP affinity changed unexpectedly: %s", imapBackend)
	}

	t.Logf("✅ Cross-protocol affinity correctly skipped (host not in pool), fell through to: %s (%s)",
		result.PreferredAddr, result.RoutingMethod)
}

// TestCrossProtocolAffinitySamePort verifies same-port scenario still works
// (e.g., all proxies connect to the same backend port — a unified proxy endpoint)
func TestCrossProtocolAffinitySamePort(t *testing.T) {
	cluster1, err := createTestCluster("cross-proto-sameport-1", 26948, []string{})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster1.Shutdown()

	affinity := server.NewAffinityManager(cluster1, true, 24*time.Hour, 1*time.Hour)
	defer affinity.Stop()

	// Both IMAP and POP3 proxies use same backend addresses (same port)
	affinity.SetBackend("user@example.com", "backend1:143", "imap")

	pop3Backends := []string{"backend1:143", "backend2:143"}
	pop3ConnMgr, err := NewConnectionManager(pop3Backends, 143, false, false, false, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create connection manager: %v", err)
	}
	pop3ConnMgr.SetAffinityManager(affinity)

	result, err := DetermineRoute(RouteParams{
		Ctx:            context.Background(),
		Username:       "user@example.com",
		Protocol:       "pop3",
		ConnManager:    pop3ConnMgr,
		EnableAffinity: true,
		ProxyName:      "POP3 Proxy",
	})
	if err != nil {
		t.Fatalf("DetermineRoute failed: %v", err)
	}

	// Same port: cross-protocol affinity should work directly
	if result.PreferredAddr != "backend1:143" {
		t.Errorf("Expected backend1:143, got '%s' (method: %s)", result.PreferredAddr, result.RoutingMethod)
	}
	if result.RoutingMethod != "affinity_cross_protocol" {
		t.Errorf("Expected 'affinity_cross_protocol', got '%s'", result.RoutingMethod)
	}

	t.Logf("✅ Cross-protocol affinity works with same-port backends: %s", result.PreferredAddr)
}

// TestCrossProtocolAffinityUnhealthyBackendFailover verifies that when a cross-protocol
// affinity resolves to an unhealthy backend in this proxy's pool, the routing correctly
// falls through to consistent hash and picks a healthy backend.
func TestCrossProtocolAffinityUnhealthyBackendFailover(t *testing.T) {
	cluster1, err := createTestCluster("cross-proto-unhealthy-1", 26949, []string{})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster1.Shutdown()

	affinity := server.NewAffinityManager(cluster1, true, 24*time.Hour, 1*time.Hour)
	defer affinity.Stop()

	// IMAP affinity points to backend1
	affinity.SetBackend("user@example.com", "backend1:143", "imap")

	// POP3 proxy has backend1:110 and backend2:110
	pop3Backends := []string{"backend1:110", "backend2:110"}
	pop3ConnMgr, err := NewConnectionManager(pop3Backends, 110, false, false, false, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create connection manager: %v", err)
	}
	pop3ConnMgr.SetAffinityManager(affinity)

	// Mark backend1:110 as unhealthy (3 consecutive failures)
	for i := 0; i < 3; i++ {
		pop3ConnMgr.RecordConnectionFailure("backend1:110")
	}
	if pop3ConnMgr.IsBackendHealthy("backend1:110") {
		t.Fatal("backend1:110 should be unhealthy after 3 failures")
	}
	// backend2:110 should still be healthy
	if !pop3ConnMgr.IsBackendHealthy("backend2:110") {
		t.Fatal("backend2:110 should be healthy")
	}

	result, err := DetermineRoute(RouteParams{
		Ctx:            context.Background(),
		Username:       "user@example.com",
		Protocol:       "pop3",
		ConnManager:    pop3ConnMgr,
		EnableAffinity: true,
		ProxyName:      "POP3 Proxy",
	})
	if err != nil {
		t.Fatalf("DetermineRoute failed: %v", err)
	}

	// Should NOT use cross-protocol affinity (resolved backend is unhealthy)
	if result.RoutingMethod == "affinity_cross_protocol" {
		t.Errorf("Should not use cross-protocol affinity when resolved backend is unhealthy, got: %s → %s",
			result.RoutingMethod, result.PreferredAddr)
	}

	// Should fall through to consistent hash and pick a healthy backend
	if result.PreferredAddr == "" {
		t.Error("Expected a preferred address from consistent hash fallback")
	}
	if result.PreferredAddr == "backend1:110" {
		t.Error("Should not route to unhealthy backend1:110")
	}
	t.Logf("Routing fell through to: %s (method: %s)", result.PreferredAddr, result.RoutingMethod)

	// IMAP affinity should be deleted (machine appears down from POP3's perspective)
	_, imapFound := affinity.GetBackend("user@example.com", "imap")
	if imapFound {
		t.Logf("Note: IMAP affinity was preserved (source protocol deletion is debatable)")
	} else {
		t.Logf("IMAP affinity was deleted (backend1 appears unhealthy)")
	}

	t.Logf("✅ Cross-protocol affinity correctly failed over when resolved backend is unhealthy")
}

// TestCrossProtocolAffinityRetryAfterStaleDelete verifies that when a same-protocol
// affinity is stale (points to unhealthy backend), the routing retries cross-protocol
// lookup to benefit from another protocol's updated affinity.
//
// Scenario: LMTP and IMAP both had affinity to backend1. Backend1 goes down.
// IMAP fails over and updates its affinity to backend2. Next LMTP connection should:
// 1. Find LMTP affinity → backend1 (unhealthy) → delete it
// 2. Retry cross-protocol → find IMAP affinity → backend2 → use it!
func TestCrossProtocolAffinityRetryAfterStaleDelete(t *testing.T) {
	cluster1, err := createTestCluster("cross-proto-retry-1", 26950, []string{})
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer cluster1.Shutdown()

	affinity := server.NewAffinityManager(cluster1, true, 24*time.Hour, 1*time.Hour)
	defer affinity.Stop()

	// Both LMTP and IMAP originally had affinity to backend1
	affinity.SetBackend("user@example.com", "backend1:24", "lmtp")
	affinity.SetBackend("user@example.com", "backend1:143", "imap")

	// Backend1 goes down. IMAP proxy detects this and fails over to backend2
	affinity.UpdateBackend("user@example.com", "backend1:143", "backend2:143", "imap")

	// Verify: IMAP now points to backend2, LMTP still points to stale backend1
	imapBackend, _ := affinity.GetBackend("user@example.com", "imap")
	lmtpBackend, _ := affinity.GetBackend("user@example.com", "lmtp")
	t.Logf("Before LMTP connection: IMAP → %s, LMTP → %s", imapBackend, lmtpBackend)

	// Create LMTP proxy's connection manager
	lmtpBackends := []string{"backend1:24", "backend2:24"}
	lmtpConnMgr, err := NewConnectionManager(lmtpBackends, 24, false, false, false, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create connection manager: %v", err)
	}
	lmtpConnMgr.SetAffinityManager(affinity)

	// Mark backend1:24 as unhealthy (backend1 is down)
	for i := 0; i < 3; i++ {
		lmtpConnMgr.RecordConnectionFailure("backend1:24")
	}

	// DetermineRoute for LMTP
	result, err := DetermineRoute(RouteParams{
		Ctx:            context.Background(),
		Username:       "user@example.com",
		Protocol:       "lmtp",
		ConnManager:    lmtpConnMgr,
		EnableAffinity: true,
		ProxyName:      "LMTP Proxy",
	})
	if err != nil {
		t.Fatalf("DetermineRoute failed: %v", err)
	}

	// LMTP should use cross-protocol affinity from IMAP (backend2) after deleting stale LMTP affinity
	if result.PreferredAddr != "backend2:24" {
		t.Errorf("Expected LMTP to route to backend2:24 (from IMAP's cross-protocol affinity), got: %s (method: %s)",
			result.PreferredAddr, result.RoutingMethod)
	}
	if result.RoutingMethod != "affinity_cross_protocol" {
		t.Errorf("Expected routing method 'affinity_cross_protocol', got: %s", result.RoutingMethod)
	}

	// LMTP's stale affinity should be deleted
	_, lmtpFound := affinity.GetBackend("user@example.com", "lmtp")
	if lmtpFound {
		t.Error("Stale LMTP affinity should have been deleted")
	}

	// IMAP affinity should still point to backend2
	imapAfter, imapFound := affinity.GetBackend("user@example.com", "imap")
	if !imapFound || imapAfter != "backend2:143" {
		t.Errorf("IMAP affinity should still point to backend2:143, got: found=%v addr=%s", imapFound, imapAfter)
	}

	t.Logf("✅ After stale LMTP affinity deleted, cross-protocol retry found IMAP's updated affinity → backend2:24")
}
