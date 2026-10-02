package main

import (
	"slices"
	"testing"
)

// TestAdminConfigCarriesSieveExtensions guards the hop from config.Config to
// AdminConfig that loadAdminConfig makes field by field: the admin tools
// validate scripts against [sieve] enabled_extensions, and a field the loader
// forgets to copy is populated by a direct decode in tests and empty in
// production (see loadAdminConfigFromString).
func TestAdminConfigCarriesSieveExtensions(t *testing.T) {
	admin := loadAdminConfigFromString(t, "[sieve]\nenabled_extensions = [\"fileinto\", \"editheader\"]\n")
	if want := []string{"fileinto", "editheader"}; !slices.Equal(admin.Sieve.EnabledExtensions, want) {
		t.Fatalf("loadAdminConfig dropped [sieve] enabled_extensions: got %v, want %v", admin.Sieve.EnabledExtensions, want)
	}
}
