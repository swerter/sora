package server

import (
	"context"
	"testing"
	"time"

	"github.com/migadu/sora/config"
	"github.com/migadu/sora/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// getMetricValue retrieves the current value of a Prometheus gauge metric
func getMetricValue(gauge *prometheus.GaugeVec, labels prometheus.Labels) float64 {
	metric := &dto.Metric{}
	if err := gauge.With(labels).Write(metric); err != nil {
		return -1
	}
	return metric.GetGauge().GetValue()
}

// TestAuthRateLimiterMetrics verifies that Prometheus metrics are correctly updated
func TestAuthRateLimiterMetrics(t *testing.T) {
	cfg := config.AuthRateLimiterConfig{
		Enabled:                  true,
		MaxAttemptsPerIPUsername: 3,
		IPUsernameBlockDuration:  5 * time.Minute,
		IPUsernameWindowDuration: 5 * time.Minute,
		MaxAttemptsPerIP:         5,
		IPBlockDuration:          15 * time.Minute,
		IPWindowDuration:         15 * time.Minute,
		MaxAttemptsPerUsername:   10,
		UsernameWindowDuration:   30 * time.Minute,
		CleanupInterval:          100 * time.Millisecond, // Fast cleanup for testing
	}

	protocol := "test-metrics"
	serverName := "test-server"
	hostname := "test-host"

	limiter := NewAuthRateLimiter(protocol, serverName, hostname, cfg)
	defer limiter.Stop()

	labels := prometheus.Labels{
		"protocol":    protocol,
		"server_name": serverName,
		"hostname":    hostname,
	}

	ctx := context.Background()
	addr1 := &StringAddr{Addr: "10.0.0.1:12345"}
	addr2 := &StringAddr{Addr: "10.0.0.2:12345"}
	user1 := "user1@example.com"
	user2 := "user2@example.com"

	// Initial state: all metrics should be 0
	time.Sleep(150 * time.Millisecond) // Wait for first cleanup cycle
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels); val != 0 {
		t.Errorf("Initial IP entries should be 0, got %f", val)
	}
	if val := getMetricValue(metrics.AuthRateLimiterBlockedIPs, labels); val != 0 {
		t.Errorf("Initial blocked IPs should be 0, got %f", val)
	}
	if val := getMetricValue(metrics.AuthRateLimiterIPUsernameEntries, labels); val != 0 {
		t.Errorf("Initial IP+username entries should be 0, got %f", val)
	}
	if val := getMetricValue(metrics.AuthRateLimiterUsernameEntries, labels); val != 0 {
		t.Errorf("Initial username entries should be 0, got %f", val)
	}

	// Record failures to trigger IP failure tracking
	limiter.RecordAuthAttempt(ctx, addr1, user1, false)
	limiter.RecordAuthAttempt(ctx, addr1, user2, false)
	limiter.RecordAuthAttempt(ctx, addr2, user1, false)

	time.Sleep(150 * time.Millisecond) // Wait for cleanup cycle to update metrics

	// Should have IP failure entries
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels); val != 2 {
		t.Errorf("Expected 2 IP entries (addr1, addr2), got %f", val)
	}

	// Should have username entries
	if val := getMetricValue(metrics.AuthRateLimiterUsernameEntries, labels); val != 2 {
		t.Errorf("Expected 2 username entries (user1, user2), got %f", val)
	}

	// Should have IP+username entries
	if val := getMetricValue(metrics.AuthRateLimiterIPUsernameEntries, labels); val != 3 {
		t.Errorf("Expected 3 IP+username entries, got %f", val)
	}

	// No blocks yet
	if val := getMetricValue(metrics.AuthRateLimiterBlockedIPs, labels); val != 0 {
		t.Errorf("Should have 0 blocked IPs before threshold, got %f", val)
	}

	// Trigger IP+username block (Tier 1) - need 3 failures for addr1+user1
	limiter.RecordAuthAttempt(ctx, addr1, user1, false) // 2nd failure
	limiter.RecordAuthAttempt(ctx, addr1, user1, false) // 3rd failure - triggers block

	time.Sleep(150 * time.Millisecond) // Wait for cleanup cycle

	// IP+username entry should now be blocked
	if val := getMetricValue(metrics.AuthRateLimiterIPUsernameEntries, labels); val != 3 {
		t.Errorf("Expected 3 IP+username entries (including blocked one), got %f", val)
	}

	// Trigger IP-only block (Tier 2) - need 5 failures from addr2
	for i := 0; i < 5; i++ {
		limiter.RecordAuthAttempt(ctx, addr2, "different-user"+string(rune(i))+"@example.com", false)
	}

	time.Sleep(150 * time.Millisecond) // Wait for cleanup cycle

	// Should have 1 blocked IP (addr2)
	if val := getMetricValue(metrics.AuthRateLimiterBlockedIPs, labels); val != 1 {
		t.Errorf("Expected 1 blocked IP after 5 failures, got %f", val)
	}

	// Verify the blocked IP is actually blocking
	if err := limiter.CanAttemptAuth(ctx, addr2, "any@example.com"); err == nil {
		t.Error("addr2 should be blocked (Tier 2)")
	}

	// Don't wait for expiry in this test - it would take too long
	// The cleanup test below specifically tests expiry behavior with short durations
	t.Logf("Test complete. Metrics verified: IP entries tracked, blocks recorded correctly")
}

// TestAuthRateLimiterMetricsCleanup verifies metrics are cleaned up when entries expire
func TestAuthRateLimiterMetricsCleanup(t *testing.T) {
	cfg := config.AuthRateLimiterConfig{
		Enabled:                  true,
		MaxAttemptsPerIPUsername: 3,
		IPUsernameBlockDuration:  200 * time.Millisecond, // Short block for testing
		IPUsernameWindowDuration: 200 * time.Millisecond,
		MaxAttemptsPerIP:         5,
		IPBlockDuration:          200 * time.Millisecond,
		IPWindowDuration:         200 * time.Millisecond,
		MaxAttemptsPerUsername:   10,
		UsernameWindowDuration:   200 * time.Millisecond,
		CleanupInterval:          50 * time.Millisecond, // Frequent cleanup
	}

	protocol := "test-cleanup"
	serverName := "test-server"
	hostname := "test-host"

	limiter := NewAuthRateLimiter(protocol, serverName, hostname, cfg)
	defer limiter.Stop()

	labels := prometheus.Labels{
		"protocol":    protocol,
		"server_name": serverName,
		"hostname":    hostname,
	}

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.0.0.100:12345"}
	user := "cleanup-test@example.com"

	// Record failures
	limiter.RecordAuthAttempt(ctx, addr, user, false)
	limiter.RecordAuthAttempt(ctx, addr, user, false)

	time.Sleep(100 * time.Millisecond) // Wait for cleanup

	// Should have entries
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels); val != 1 {
		t.Errorf("Expected 1 IP entry, got %f", val)
	}

	// Wait for entries to expire and be cleaned up
	time.Sleep(300 * time.Millisecond) // Window duration + cleanup cycles

	// Metrics should return to 0
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels); val != 0 {
		t.Errorf("Expected 0 IP entries after expiry, got %f", val)
	}
	if val := getMetricValue(metrics.AuthRateLimiterIPUsernameEntries, labels); val != 0 {
		t.Errorf("Expected 0 IP+username entries after expiry, got %f", val)
	}
	if val := getMetricValue(metrics.AuthRateLimiterUsernameEntries, labels); val != 0 {
		t.Errorf("Expected 0 username entries after expiry, got %f", val)
	}
}

// TestAuthRateLimiterMetricsMultipleProtocols verifies metrics are tracked per protocol
func TestAuthRateLimiterMetricsMultipleProtocols(t *testing.T) {
	cfg := config.AuthRateLimiterConfig{
		Enabled:          true,
		MaxAttemptsPerIP: 3,
		IPBlockDuration:  5 * time.Minute,
		IPWindowDuration: 5 * time.Minute,
		CleanupInterval:  100 * time.Millisecond,
	}

	hostname := "test-host"
	limiter1 := NewAuthRateLimiter("imap", "server1", hostname, cfg)
	defer limiter1.Stop()
	limiter2 := NewAuthRateLimiter("pop3", "server2", hostname, cfg)
	defer limiter2.Stop()

	labels1 := prometheus.Labels{
		"protocol":    "imap",
		"server_name": "server1",
		"hostname":    hostname,
	}
	labels2 := prometheus.Labels{
		"protocol":    "pop3",
		"server_name": "server2",
		"hostname":    hostname,
	}

	ctx := context.Background()
	addr := &StringAddr{Addr: "10.0.0.50:12345"}

	// Record failures on protocol 1 only
	limiter1.RecordAuthAttempt(ctx, addr, "user@example.com", false)
	time.Sleep(150 * time.Millisecond)

	// Protocol 1 should have entries
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels1); val != 1 {
		t.Errorf("Protocol 1 should have 1 IP entry, got %f", val)
	}

	// Protocol 2 should have no entries
	if val := getMetricValue(metrics.AuthRateLimiterIPEntries, labels2); val != 0 {
		t.Errorf("Protocol 2 should have 0 IP entries, got %f", val)
	}
}
