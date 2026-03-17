package server

import (
	"context"
	"testing"
	"time"

	"github.com/migadu/sora/config"
)

// newTestLimiter creates a rate limiter configured for fast testing.
func newTestLimiter(protocol, name string) *AuthRateLimiter {
	cfg := config.AuthRateLimiterConfig{
		Enabled:                  true,
		MaxAttemptsPerIPUsername: 2,
		IPUsernameBlockDuration:  1 * time.Minute,
		IPUsernameWindowDuration: 5 * time.Minute,
		MaxAttemptsPerIP:         3,
		IPBlockDuration:          2 * time.Minute,
		IPWindowDuration:         10 * time.Minute,
		CleanupInterval:          1 * time.Hour, // Don't auto-clean in tests
	}
	return NewAuthRateLimiter(protocol, name, "localhost", cfg)
}

// --- GetBlockedEntries tests ---

func TestGetBlockedEntries_NilLimiter(t *testing.T) {
	var limiter *AuthRateLimiter
	entries := limiter.GetBlockedEntries()
	if entries != nil {
		t.Errorf("Expected nil for nil limiter, got %v", entries)
	}
}

func TestGetBlockedEntries_NoBlocks(t *testing.T) {
	limiter := newTestLimiter("test", "srv1")
	defer limiter.Stop()

	entries := limiter.GetBlockedEntries()
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}

func TestGetBlockedEntries_IPUsernameBlock(t *testing.T) {
	limiter := newTestLimiter("test-imap", "srv1")
	defer limiter.Stop()

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.0.0.1:1234"}

	// Trigger IP+username block (2 failures)
	limiter.RecordAuthAttempt(ctx, addr, "user@example.com", false)
	limiter.RecordAuthAttempt(ctx, addr, "user@example.com", false)

	entries := limiter.GetBlockedEntries()

	// Should have at least 1 ip_username entry
	found := false
	for _, e := range entries {
		if e.Type == "ip_username" && e.Username == "user@example.com" && e.IP == "10.0.0.1" {
			found = true
			if e.FailureCount != 2 {
				t.Errorf("Expected failure count 2, got %d", e.FailureCount)
			}
			if e.BlockedUntil.Before(time.Now()) {
				t.Error("BlockedUntil should be in the future")
			}
		}
	}
	if !found {
		t.Errorf("Expected ip_username block for user@example.com, entries: %+v", entries)
	}
}

func TestGetBlockedEntries_IPBlock(t *testing.T) {
	limiter := newTestLimiter("test-pop3", "srv1")
	defer limiter.Stop()

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.0.0.2:5678"}

	// Trigger IP block (3 failures with different usernames to avoid ip_username block interfering)
	limiter.RecordAuthAttempt(ctx, addr, "a@example.com", false)
	limiter.RecordAuthAttempt(ctx, addr, "b@example.com", false)
	limiter.RecordAuthAttempt(ctx, addr, "c@example.com", false)

	entries := limiter.GetBlockedEntries()

	foundIP := false
	for _, e := range entries {
		if e.Type == "ip" && e.IP == "10.0.0.2" {
			foundIP = true
			if e.BlockedUntil.Before(time.Now()) {
				t.Error("IP BlockedUntil should be in the future")
			}
		}
	}
	if !foundIP {
		t.Errorf("Expected ip block for 10.0.0.2, entries: %+v", entries)
	}
}

func TestGetBlockedEntries_ExpiredNotReturned(t *testing.T) {
	limiter := newTestLimiter("test", "srv1")
	defer limiter.Stop()

	// Inject an expired block directly
	limiter.ipMu.Lock()
	limiter.blockedIPs["192.168.99.99"] = &BlockedIPInfo{
		BlockedUntil: time.Now().Add(-1 * time.Hour), // Already expired
		FailureCount: 5,
		FirstFailure: time.Now().Add(-2 * time.Hour),
		LastFailure:  time.Now().Add(-1 * time.Hour),
		Protocol:     "test",
	}
	limiter.ipMu.Unlock()

	entries := limiter.GetBlockedEntries()
	for _, e := range entries {
		if e.IP == "192.168.99.99" {
			t.Error("Expired block should not be returned by GetBlockedEntries")
		}
	}
}

// --- Registry tests ---

func TestRegisterAndGetAllBlockedEntries(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	limiter1 := newTestLimiter("imap", "backend1")
	defer limiter1.Stop()
	limiter2 := newTestLimiter("pop3", "backend1")
	defer limiter2.Stop()

	RegisterRateLimiter("imap", "backend1", limiter1)
	RegisterRateLimiter("pop3", "backend1", limiter2)

	ctx := context.Background()
	addr1 := &StringAddr{Addr: "10.1.1.1:1000"}
	addr2 := &StringAddr{Addr: "10.2.2.2:2000"}

	// Trigger blocks in both limiters
	limiter1.RecordAuthAttempt(ctx, addr1, "user1@example.com", false)
	limiter1.RecordAuthAttempt(ctx, addr1, "user1@example.com", false)

	limiter2.RecordAuthAttempt(ctx, addr2, "user2@example.com", false)
	limiter2.RecordAuthAttempt(ctx, addr2, "user2@example.com", false)

	entries := GetAllBlockedEntries()
	if len(entries) < 2 {
		t.Errorf("Expected at least 2 blocked entries from 2 limiters, got %d: %+v", len(entries), entries)
	}
}

func TestGetBlockedEntriesByProtocol(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	limiterIMAP := newTestLimiter("imap", "srv1")
	defer limiterIMAP.Stop()
	limiterPOP3 := newTestLimiter("pop3", "srv1")
	defer limiterPOP3.Stop()

	RegisterRateLimiter("imap", "srv1", limiterIMAP)
	RegisterRateLimiter("pop3", "srv1", limiterPOP3)

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.5.5.5:9999"}

	// Only trigger block in IMAP limiter
	limiterIMAP.RecordAuthAttempt(ctx, addr, "victim@example.com", false)
	limiterIMAP.RecordAuthAttempt(ctx, addr, "victim@example.com", false)

	imapEntries := GetBlockedEntriesByProtocol("imap")
	pop3Entries := GetBlockedEntriesByProtocol("pop3")

	if len(imapEntries) == 0 {
		t.Error("Expected blocked entries for imap protocol")
	}
	if len(pop3Entries) != 0 {
		t.Errorf("Expected 0 blocked entries for pop3, got %d", len(pop3Entries))
	}
}

func TestUnregisterRateLimiter(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	limiter := newTestLimiter("imap", "srv1")
	defer limiter.Stop()

	RegisterRateLimiter("imap", "srv1", limiter)

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.9.9.9:1111"}
	limiter.RecordAuthAttempt(ctx, addr, "test@example.com", false)
	limiter.RecordAuthAttempt(ctx, addr, "test@example.com", false)

	// Verify entries exist
	entries := GetAllBlockedEntries()
	if len(entries) == 0 {
		t.Fatal("Expected blocked entries before unregister")
	}

	// Unregister
	UnregisterRateLimiter("imap", "srv1")

	// After unregister, entries should be gone from registry
	entries = GetAllBlockedEntries()
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after unregister, got %d", len(entries))
	}
}

func TestRegisterNilLimiter(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	// Should not panic
	RegisterRateLimiter("imap", "srv1", nil)

	globalRateLimiterRegistry.mu.RLock()
	count := len(globalRateLimiterRegistry.limiters)
	globalRateLimiterRegistry.mu.RUnlock()

	if count != 0 {
		t.Errorf("Nil limiter should not be registered, got %d entries", count)
	}
}

func TestGetAllBlockedEntries_EmptyReturnsEmptySlice(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	entries := GetAllBlockedEntries()
	if entries == nil {
		t.Error("Expected empty slice (not nil) for JSON serialization")
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}

func TestGetBlockedEntriesByProtocol_EmptyReturnsEmptySlice(t *testing.T) {
	// Clean global registry state
	globalRateLimiterRegistry.mu.Lock()
	savedLimiters := globalRateLimiterRegistry.limiters
	globalRateLimiterRegistry.limiters = make(map[string]*AuthRateLimiter)
	globalRateLimiterRegistry.mu.Unlock()
	defer func() {
		globalRateLimiterRegistry.mu.Lock()
		globalRateLimiterRegistry.limiters = savedLimiters
		globalRateLimiterRegistry.mu.Unlock()
	}()

	entries := GetBlockedEntriesByProtocol("nonexistent")
	if entries == nil {
		t.Error("Expected empty slice (not nil) for JSON serialization")
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}
