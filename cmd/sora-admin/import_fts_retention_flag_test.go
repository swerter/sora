package main

import (
	"testing"
	"time"
)

// --fts-retention overrides [cleanup] fts_retention; without it an import uses the same
// window the server applies to delivered mail.
func TestResolveImportFTSRetention(t *testing.T) {
	saved := globalConfig.Cleanup.FTSRetention
	t.Cleanup(func() { globalConfig.Cleanup.FTSRetention = saved })

	for _, tc := range []struct {
		name, config, flag string
		want               time.Duration
		wantErr            bool
	}{
		{"nothing set indexes everything", "", "", 0, false},
		{"config value is the default", "30d", "", 30 * 24 * time.Hour, false},
		{"flag overrides the config", "30d", "180d", 180 * 24 * time.Hour, false},
		{"flag accepts Go durations", "", "4320h", 4320 * time.Hour, false},
		{"flag 0 turns it off despite the config", "30d", "0", 0, false},
		{"garbage is rejected", "", "six months", 0, true},
		{"negative is rejected", "", "-5d", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			globalConfig.Cleanup.FTSRetention = tc.config
			got, err := resolveImportFTSRetention(tc.flag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error for %q, got %v", tc.flag, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
