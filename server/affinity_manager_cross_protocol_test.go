package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/migadu/sora/cluster"
	"github.com/migadu/sora/config"
)

// TestAffinityManager_CrossProtocol tests cross-protocol affinity for cache locality
func TestAffinityManager_CrossProtocol(t *testing.T) {
	// Create cluster manager for testing
	clusterCfg := config.ClusterConfig{
		Enabled:   true,
		Addr:      "127.0.0.1:27946",
		NodeID:    "test-cross-protocol",
		Peers:     []string{},
		SecretKey: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=", // Base64-encoded 32-byte key
	}

	clusterMgr, err := cluster.New(clusterCfg)
	if err != nil {
		t.Fatalf("Failed to create cluster manager: %v", err)
	}
	defer clusterMgr.Shutdown()

	// Create affinity manager
	am := NewAffinityManager(clusterMgr, true, 24*time.Hour, 1*time.Hour)
	if am == nil {
		t.Fatal("Expected affinity manager to be created")
	}
	defer am.Stop()

	username := "test@example.com"

	t.Run("SameProtocol", func(t *testing.T) {
		// Set affinity for IMAP
		am.SetBackend(username, "backend1:143", "imap")

		// Get affinity for IMAP (same protocol)
		backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, "imap")
		if !found {
			t.Fatal("Expected to find IMAP affinity")
		}
		if backend != "backend1:143" {
			t.Errorf("Expected backend1:143, got %s", backend)
		}
		if foundProtocol != "imap" {
			t.Errorf("Expected protocol 'imap', got %s", foundProtocol)
		}
	})

	t.Run("CrossProtocol_IMAP_to_POP3", func(t *testing.T) {
		// User has IMAP affinity, connecting via POP3
		backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, "pop3")
		if !found {
			t.Fatal("Expected to find cross-protocol affinity (IMAP -> POP3)")
		}
		if backend != "backend1:143" {
			t.Errorf("Expected backend1:143 from IMAP affinity, got %s", backend)
		}
		if foundProtocol != "imap" {
			t.Errorf("Expected to find affinity from 'imap', got %s", foundProtocol)
		}
		t.Logf("✓ Cross-protocol affinity: POP3 routed to backend from IMAP affinity (%s)", backend)
	})

	t.Run("CrossProtocol_IMAP_to_LMTP", func(t *testing.T) {
		// User has IMAP affinity, mail delivery via LMTP
		backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, "lmtp")
		if !found {
			t.Fatal("Expected to find cross-protocol affinity (IMAP -> LMTP)")
		}
		if backend != "backend1:143" {
			t.Errorf("Expected backend1:143 from IMAP affinity, got %s", backend)
		}
		if foundProtocol != "imap" {
			t.Errorf("Expected to find affinity from 'imap', got %s", foundProtocol)
		}
		t.Logf("✓ Cross-protocol affinity: LMTP routed to backend from IMAP affinity (%s)", backend)
	})

	t.Run("ProtocolSpecificOverride", func(t *testing.T) {
		// Set specific POP3 affinity (different backend)
		am.SetBackend(username, "backend2:110", "pop3")

		// POP3 should use its own affinity, not IMAP's
		backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, "pop3")
		if !found {
			t.Fatal("Expected to find POP3 affinity")
		}
		if backend != "backend2:110" {
			t.Errorf("Expected backend2:110 from POP3-specific affinity, got %s", backend)
		}
		if foundProtocol != "pop3" {
			t.Errorf("Expected protocol 'pop3', got %s", foundProtocol)
		}
		t.Logf("✓ Protocol-specific affinity takes precedence over cross-protocol")
	})

	t.Run("NoAffinity", func(t *testing.T) {
		// User with no affinity
		otherUser := "other@example.com"
		backend, foundProtocol, found := am.GetBackendAcrossProtocols(otherUser, "imap")
		if found {
			t.Errorf("Expected no affinity, but found %s from protocol %s", backend, foundProtocol)
		}
	})

	t.Run("ExpiredAffinity", func(t *testing.T) {
		// Create affinity manager with very short TTL for testing
		shortTTLMgr := NewAffinityManager(clusterMgr, true, 100*time.Millisecond, 1*time.Hour)
		defer shortTTLMgr.Stop()

		expiredUser := "expired@example.com"
		shortTTLMgr.SetBackend(expiredUser, "backend3:143", "imap")

		// Verify affinity exists
		backend, _, found := shortTTLMgr.GetBackendAcrossProtocols(expiredUser, "imap")
		if !found || backend != "backend3:143" {
			t.Fatal("Expected to find affinity before expiration")
		}

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		// Affinity should be expired
		_, _, found = shortTTLMgr.GetBackendAcrossProtocols(expiredUser, "imap")
		if found {
			t.Error("Expected affinity to be expired")
		}

		// Cross-protocol lookup should also return nothing
		_, _, found = shortTTLMgr.GetBackendAcrossProtocols(expiredUser, "pop3")
		if found {
			t.Error("Expected cross-protocol affinity to be expired")
		}
		t.Logf("✓ Expired affinity not returned")
	})
}

// TestAffinityManager_ProtocolPriority tests the protocol priority order
func TestAffinityManager_ProtocolPriority(t *testing.T) {
	clusterCfg := config.ClusterConfig{
		Enabled:   true,
		Addr:      "127.0.0.1:27947",
		NodeID:    "test-priority",
		Peers:     []string{},
		SecretKey: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=",
	}

	clusterMgr, err := cluster.New(clusterCfg)
	if err != nil {
		t.Fatalf("Failed to create cluster manager: %v", err)
	}
	defer clusterMgr.Shutdown()

	am := NewAffinityManager(clusterMgr, true, 24*time.Hour, 1*time.Hour)
	if am == nil {
		t.Fatal("Expected affinity manager to be created")
	}
	defer am.Stop()

	username := "priority@example.com"

	// Set affinity for multiple protocols
	am.SetBackend(username, "backend-imap:143", "imap")
	am.SetBackend(username, "backend-pop3:110", "pop3")
	am.SetBackend(username, "backend-lmtp:24", "lmtp")
	am.SetBackend(username, "backend-sieve:4190", "managesieve")

	// When requesting each protocol, it should prefer its own affinity
	tests := []struct {
		protocol        string
		expectedBackend string
		expectedProto   string
	}{
		{"imap", "backend-imap:143", "imap"},
		{"pop3", "backend-pop3:110", "pop3"},
		{"lmtp", "backend-lmtp:24", "lmtp"},
		{"managesieve", "backend-sieve:4190", "managesieve"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Protocol_%s", tt.protocol), func(t *testing.T) {
			backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, tt.protocol)
			if !found {
				t.Fatalf("Expected to find affinity for %s", tt.protocol)
			}
			if backend != tt.expectedBackend {
				t.Errorf("For %s: expected %s, got %s", tt.protocol, tt.expectedBackend, backend)
			}
			if foundProtocol != tt.expectedProto {
				t.Errorf("For %s: expected protocol %s, got %s", tt.protocol, tt.expectedProto, foundProtocol)
			}
			t.Logf("✓ Protocol %s correctly uses its own affinity: %s", tt.protocol, backend)
		})
	}

	// Delete IMAP affinity
	am.DeleteBackend(username, "imap")

	// Now when requesting IMAP, it should fall back to POP3 (next in priority)
	backend, foundProtocol, found := am.GetBackendAcrossProtocols(username, "imap")
	if !found {
		t.Fatal("Expected to find cross-protocol affinity after IMAP deletion")
	}
	if backend != "backend-pop3:110" {
		t.Errorf("Expected fallback to POP3 backend, got %s", backend)
	}
	if foundProtocol != "pop3" {
		t.Errorf("Expected to find affinity from 'pop3', got %s", foundProtocol)
	}
	t.Logf("✓ After deleting IMAP affinity, correctly falls back to POP3: %s", backend)
}
