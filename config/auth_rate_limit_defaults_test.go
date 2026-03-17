package config

import (
	"os"
	"testing"
	"time"
)

// TestAuthRateLimitDefaults verifies that enabling auth_rate_limit.enabled = true
// automatically applies all default values without requiring users to specify them
func TestAuthRateLimitDefaults(t *testing.T) {
	// Create a minimal TOML config with only enabled = true
	configContent := `
[[server]]
type = "imap_proxy"
name = "test-proxy"
addr = ":993"
remote_addrs = ["backend:143"]
auth_rate_limit.enabled = true
`

	// Write to temp file
	tmpfile, err := os.CreateTemp("", "config-test-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	// Load config
	var cfg Config
	if err := LoadConfigFromFile(tmpfile.Name(), &cfg); err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify we have the server
	if len(cfg.DynamicServers) != 1 {
		t.Fatalf("Expected 1 dynamic server, got %d", len(cfg.DynamicServers))
	}

	server := cfg.DynamicServers[0]
	if server.AuthRateLimit == nil {
		t.Fatal("AuthRateLimit should not be nil")
	}

	rlCfg := *server.AuthRateLimit

	// Verify enabled is set
	if !rlCfg.Enabled {
		t.Error("Expected Enabled = true")
	}

	// Verify defaults were applied
	defaults := DefaultAuthRateLimiterConfig()

	if rlCfg.MaxAttemptsPerIPUsername != defaults.MaxAttemptsPerIPUsername {
		t.Errorf("MaxAttemptsPerIPUsername: got %d, want %d", rlCfg.MaxAttemptsPerIPUsername, defaults.MaxAttemptsPerIPUsername)
	}
	if rlCfg.IPUsernameBlockDuration != defaults.IPUsernameBlockDuration {
		t.Errorf("IPUsernameBlockDuration: got %v, want %v", rlCfg.IPUsernameBlockDuration, defaults.IPUsernameBlockDuration)
	}
	if rlCfg.IPUsernameWindowDuration != defaults.IPUsernameWindowDuration {
		t.Errorf("IPUsernameWindowDuration: got %v, want %v", rlCfg.IPUsernameWindowDuration, defaults.IPUsernameWindowDuration)
	}
	if rlCfg.MaxAttemptsPerIP != defaults.MaxAttemptsPerIP {
		t.Errorf("MaxAttemptsPerIP: got %d, want %d", rlCfg.MaxAttemptsPerIP, defaults.MaxAttemptsPerIP)
	}
	if rlCfg.IPBlockDuration != defaults.IPBlockDuration {
		t.Errorf("IPBlockDuration: got %v, want %v", rlCfg.IPBlockDuration, defaults.IPBlockDuration)
	}
	if rlCfg.IPWindowDuration != defaults.IPWindowDuration {
		t.Errorf("IPWindowDuration: got %v, want %v", rlCfg.IPWindowDuration, defaults.IPWindowDuration)
	}
	if rlCfg.MaxAttemptsPerUsername != defaults.MaxAttemptsPerUsername {
		t.Errorf("MaxAttemptsPerUsername: got %d, want %d", rlCfg.MaxAttemptsPerUsername, defaults.MaxAttemptsPerUsername)
	}
	if rlCfg.UsernameWindowDuration != defaults.UsernameWindowDuration {
		t.Errorf("UsernameWindowDuration: got %v, want %v", rlCfg.UsernameWindowDuration, defaults.UsernameWindowDuration)
	}
	if rlCfg.CleanupInterval != defaults.CleanupInterval {
		t.Errorf("CleanupInterval: got %v, want %v", rlCfg.CleanupInterval, defaults.CleanupInterval)
	}
	if rlCfg.DelayStartThreshold != defaults.DelayStartThreshold {
		t.Errorf("DelayStartThreshold: got %d, want %d", rlCfg.DelayStartThreshold, defaults.DelayStartThreshold)
	}
	if rlCfg.InitialDelay != defaults.InitialDelay {
		t.Errorf("InitialDelay: got %v, want %v", rlCfg.InitialDelay, defaults.InitialDelay)
	}
	if rlCfg.MaxDelay != defaults.MaxDelay {
		t.Errorf("MaxDelay: got %v, want %v", rlCfg.MaxDelay, defaults.MaxDelay)
	}
	if rlCfg.DelayMultiplier != defaults.DelayMultiplier {
		t.Errorf("DelayMultiplier: got %f, want %f", rlCfg.DelayMultiplier, defaults.DelayMultiplier)
	}
	if rlCfg.CacheCleanupInterval != defaults.CacheCleanupInterval {
		t.Errorf("CacheCleanupInterval: got %v, want %v", rlCfg.CacheCleanupInterval, defaults.CacheCleanupInterval)
	}
	if rlCfg.MaxIPUsernameEntries != defaults.MaxIPUsernameEntries {
		t.Errorf("MaxIPUsernameEntries: got %d, want %d", rlCfg.MaxIPUsernameEntries, defaults.MaxIPUsernameEntries)
	}
	if rlCfg.MaxIPEntries != defaults.MaxIPEntries {
		t.Errorf("MaxIPEntries: got %d, want %d", rlCfg.MaxIPEntries, defaults.MaxIPEntries)
	}
	if rlCfg.MaxUsernameEntries != defaults.MaxUsernameEntries {
		t.Errorf("MaxUsernameEntries: got %d, want %d", rlCfg.MaxUsernameEntries, defaults.MaxUsernameEntries)
	}
}

// TestAuthRateLimitCustomValues verifies that explicitly set values override defaults
func TestAuthRateLimitCustomValues(t *testing.T) {
	configContent := `
[[server]]
type = "imap_proxy"
name = "test-proxy"
addr = ":993"
remote_addrs = ["backend:143"]
auth_rate_limit.enabled = true
auth_rate_limit.max_attempts_per_ip_username = 3
auth_rate_limit.ip_username_block_duration = "5m"
auth_rate_limit.max_attempts_per_ip = 10
`

	tmpfile, err := os.CreateTemp("", "config-test-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadConfigFromFile(tmpfile.Name(), &cfg); err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	server := cfg.DynamicServers[0]
	rlCfg := *server.AuthRateLimit

	// Verify custom values are preserved
	if rlCfg.MaxAttemptsPerIPUsername != 3 {
		t.Errorf("MaxAttemptsPerIPUsername: got %d, want 3", rlCfg.MaxAttemptsPerIPUsername)
	}
	if rlCfg.IPUsernameBlockDuration != 5*time.Minute {
		t.Errorf("IPUsernameBlockDuration: got %v, want 5m", rlCfg.IPUsernameBlockDuration)
	}
	if rlCfg.MaxAttemptsPerIP != 10 {
		t.Errorf("MaxAttemptsPerIP: got %d, want 10", rlCfg.MaxAttemptsPerIP)
	}

	// Verify unset values still get defaults
	defaults := DefaultAuthRateLimiterConfig()
	if rlCfg.IPUsernameWindowDuration != defaults.IPUsernameWindowDuration {
		t.Errorf("IPUsernameWindowDuration should use default: got %v, want %v", rlCfg.IPUsernameWindowDuration, defaults.IPUsernameWindowDuration)
	}
	if rlCfg.IPBlockDuration != defaults.IPBlockDuration {
		t.Errorf("IPBlockDuration should use default: got %v, want %v", rlCfg.IPBlockDuration, defaults.IPBlockDuration)
	}
}

// TestAuthRateLimitDisabled verifies that disabled rate limiters don't get defaults applied
func TestAuthRateLimitDisabled(t *testing.T) {
	configContent := `
[[server]]
type = "imap_proxy"
name = "test-proxy"
addr = ":993"
remote_addrs = ["backend:143"]
auth_rate_limit.enabled = false
`

	tmpfile, err := os.CreateTemp("", "config-test-*.toml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(configContent)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadConfigFromFile(tmpfile.Name(), &cfg); err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	server := cfg.DynamicServers[0]
	rlCfg := *server.AuthRateLimit

	// Verify enabled is false
	if rlCfg.Enabled {
		t.Error("Expected Enabled = false")
	}

	// Verify defaults were NOT applied (should remain zero values)
	if rlCfg.MaxAttemptsPerIPUsername != 0 {
		t.Errorf("MaxAttemptsPerIPUsername should be 0 when disabled, got %d", rlCfg.MaxAttemptsPerIPUsername)
	}
	if rlCfg.MaxAttemptsPerIP != 0 {
		t.Errorf("MaxAttemptsPerIP should be 0 when disabled, got %d", rlCfg.MaxAttemptsPerIP)
	}
}
