//go:build integration

package managesieve

import (
	"bufio"
	"context"
	"net"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/migadu/sora/integration_tests/common"
	"github.com/migadu/sora/server/managesieve"
	"github.com/migadu/sora/server/sieveengine"
)

func TestManageSieveConfigurableExtensions(t *testing.T) {
	common.SkipIfDatabaseUnavailable(t)

	// Test 1: No extensions configured - advertises the default set, the one
	// delivery compiles scripts with (sieveengine.EffectiveExtensions).
	t.Run("NoExtensions", func(t *testing.T) {
		testExtensions(t, []string{}, sieveengine.DefaultSieveExtensions)
	})

	// Names the engine does not support are dropped; a list with nothing
	// supported advertises the default set rather than nothing.
	t.Run("UnsupportedNamesDropped", func(t *testing.T) {
		testExtensions(t, []string{"fileinto", "enotify", "vacation"}, []string{"fileinto", "vacation"})
	})
	t.Run("NothingSupportedFallsBack", func(t *testing.T) {
		testExtensions(t, []string{"Fileinto", "vacation "}, sieveengine.DefaultSieveExtensions)
	})

	// Test 2: Single extension
	t.Run("SingleExtension", func(t *testing.T) {
		configuredExtensions := []string{"vacation"}
		testExtensions(t, configuredExtensions, configuredExtensions)
	})

	// Test 3: Multiple extensions
	t.Run("MultipleExtensions", func(t *testing.T) {
		configuredExtensions := []string{"fileinto", "vacation", "regex"}
		testExtensions(t, configuredExtensions, configuredExtensions)
	})

	// Test 4: All supported extensions
	t.Run("AllSupportedExtensions", func(t *testing.T) {
		configuredExtensions := []string{"fileinto", "vacation", "envelope", "imap4flags", "variables", "relational", "copy", "regex", "date", "index", "encoded-character"}
		testExtensions(t, configuredExtensions, configuredExtensions)
	})
}

func testExtensions(t *testing.T, configuredExtensions []string, expectedExtensions []string) {
	t.Helper()

	// Create test database and account
	rdb := common.SetupTestDatabase(t)
	_ = common.CreateTestAccount(t, rdb) // Create account for database setup
	address := common.GetRandomAddress(t)

	// Create ManageSieve server with specific extensions
	options := managesieve.ManageSieveServerOptions{
		InsecureAuth: true, // Enable PLAIN auth for testing
	}
	if configuredExtensions != nil {
		options.SupportedExtensions = configuredExtensions
	}

	server, err := managesieve.New(
		context.Background(),
		"test",
		"localhost",
		address,
		rdb,
		options,
	)
	if err != nil {
		t.Fatalf("Failed to create ManageSieve server: %v", err)
	}

	errChan := make(chan error, 1)
	go func() {
		server.Start(errChan)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	defer func() {
		server.Close()
		select {
		case <-errChan:
		default:
		}
	}()

	// Connect to the server
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Failed to connect to ManageSieve server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Read greeting and check capabilities
	greeting := readResponse(t, reader)
	t.Logf("Server greeting: %s", strings.TrimSpace(greeting))

	// Send CAPABILITY command
	sendCommand(t, writer, "CAPABILITY")
	capabilityResponse := readResponse(t, reader)
	t.Logf("CAPABILITY response: %s", strings.TrimSpace(capabilityResponse))

	// Extract SIEVE capability line
	sieveLine := ""
	for _, line := range strings.Split(capabilityResponse, "\n") {
		if strings.Contains(line, "\"SIEVE\"") {
			sieveLine = line
			break
		}
	}

	if sieveLine == "" {
		t.Fatalf("No SIEVE capability line found in response: %s", capabilityResponse)
	}

	t.Logf("SIEVE capability line: %s", sieveLine)

	// The advertised list must be exactly the expected set: nothing missing,
	// nothing extra (a client builds rules from it, and the backend refuses
	// what is not in it).
	// The capability response is one line; the extensions are the quoted
	// string after "SIEVE".
	const key = `"SIEVE" "`
	start := strings.Index(sieveLine, key)
	if start < 0 {
		t.Fatalf("no SIEVE value in: %s", sieveLine)
	}
	rest := sieveLine[start+len(key):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		t.Fatalf("unterminated SIEVE value in: %s", sieveLine)
	}
	advertised := strings.Fields(rest[:end])
	want := append([]string(nil), expectedExtensions...)
	sort.Strings(advertised)
	sort.Strings(want)
	if !slices.Equal(advertised, want) {
		t.Fatalf("SIEVE capability advertises %v, want %v (line: %s)", advertised, want, strings.TrimSpace(sieveLine))
	}

	t.Logf("Successfully verified %d extensions: %v", len(expectedExtensions), expectedExtensions)
}
