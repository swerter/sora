package sieveengine

import (
	"slices"
	"testing"
)

// TestEffectiveExtensions pins the one resolution every ingress path and
// capability report uses.
func TestEffectiveExtensions(t *testing.T) {
	cases := []struct {
		name       string
		configured []string
		want       []string
	}{
		{"nothing configured", nil, DefaultSieveExtensions},
		{"empty list", []string{}, DefaultSieveExtensions},
		{"supported names kept in order", []string{"vacation", "fileinto", "editheader"}, []string{"vacation", "fileinto", "editheader"}},
		{"unsupported names dropped", []string{"fileinto", "enotify", "vacation"}, []string{"fileinto", "vacation"}},
		// A list of typos must not leave delivery with no extensions at all, which
		// would fail every script with a require and the embedded default script.
		{"nothing supported falls back", []string{"Fileinto", "vacation "}, DefaultSieveExtensions},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EffectiveExtensions(tc.configured); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	if got := InvalidExtensions([]string{"fileinto", "enotify", "x"}); !slices.Equal(got, []string{"enotify", "x"}) {
		t.Fatalf("InvalidExtensions = %v", got)
	}
}
