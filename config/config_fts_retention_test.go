package config

import (
	"testing"
	"time"
)

// --- GetFTSRetention (vector retention) tests ---

func TestGetFTSRetention_EmptyDefaultsToZero(t *testing.T) {
	cfg := CleanupConfig{}
	d, err := cfg.GetFTSRetention()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 0 {
		t.Errorf("expected 0 (keep forever), got %v", d)
	}
}

func TestGetFTSRetention_ExplicitValue(t *testing.T) {
	cfg := CleanupConfig{FTSRetention: "1095d"}
	d, err := cfg.GetFTSRetention()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := 1095 * 24 * time.Hour
	if d != expected {
		t.Errorf("expected %v, got %v", expected, d)
	}
}

func TestGetFTSRetention_InvalidValue(t *testing.T) {
	cfg := CleanupConfig{FTSRetention: "notaduration"}
	_, err := cfg.GetFTSRetention()
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestGetFTSRetentionWithDefault_InvalidFallsBackToZero(t *testing.T) {
	cfg := CleanupConfig{FTSRetention: "bad"}
	d := cfg.GetFTSRetentionWithDefault()
	if d != 0 {
		t.Errorf("expected 0 (keep forever) fallback, got %v", d)
	}
}

// --- Default config coherence ---

func TestNewDefaultConfig_FTSRetentionDefaults(t *testing.T) {
	cfg := NewDefaultConfig()

	// FTSRetention (vector) should be empty (keep forever)
	if cfg.Cleanup.FTSRetention != "" {
		t.Errorf("expected empty FTSRetention in defaults, got %q", cfg.Cleanup.FTSRetention)
	}

	// Verify parsed values match expectations
	vectorRetention := cfg.Cleanup.GetFTSRetentionWithDefault()
	if vectorRetention != 0 {
		t.Errorf("expected vector retention 0 (keep forever), got %v", vectorRetention)
	}
}
