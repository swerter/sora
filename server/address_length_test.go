package server

import (
	"strings"
	"testing"
)

func TestAddressLengthValidation(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		errString string
	}{
		{
			name:    "valid address within limits",
			input:   "user@example.com",
			wantErr: false,
		},
		{
			name:    "local part at RFC 5321 length (64 chars) - allowed",
			input:   strings.Repeat("a", 64) + "@example.com",
			wantErr: false,
		},
		{
			name:    "SRS-length local part (80 chars) - allowed",
			input:   strings.Repeat("a", 80) + "@example.com",
			wantErr: false,
		},
		{
			name:    "local part at max length (128 chars)",
			input:   strings.Repeat("a", 128) + "@example.com",
			wantErr: false,
		},
		{
			name:      "local part exceeds max length (129 chars)",
			input:     strings.Repeat("a", 129) + "@example.com",
			wantErr:   true,
			errString: "local part exceeds maximum length of 128 characters",
		},
		{
			name:      "local part way over max (1000 chars)",
			input:     strings.Repeat("a", 1000) + "@example.com",
			wantErr:   true,
			errString: "local part exceeds maximum length of 128 characters",
		},
		{
			name:    "real-world SRS address",
			input:   "srs0=jpm1hc=br=promotions.papajohns.com=msprvs1=20536anqcddxi=bounces-266693-994@cyberwolfsystems.tech",
			wantErr: false,
		},
		{
			name:    "domain at max length (255 chars)",
			input:   "user@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 59) + ".abc",
			wantErr: false,
		},
		{
			name:      "domain exceeds max length (256 chars)",
			input:     "user@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 60) + ".abc",
			wantErr:   true,
			errString: "domain exceeds maximum length of 255 characters",
		},
		{
			name:      "domain way over max (1000 chars)",
			input:     "user@" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "." + strings.Repeat("e", 63) + "." + strings.Repeat("f", 63) + "." + strings.Repeat("g", 63) + "." + strings.Repeat("h", 63) + "." + strings.Repeat("i", 63) + "." + strings.Repeat("j", 63) + "." + strings.Repeat("k", 63) + "." + strings.Repeat("l", 63) + "." + strings.Repeat("m", 63) + "." + strings.Repeat("n", 63) + "." + strings.Repeat("o", 63) + "." + strings.Repeat("p", 63) + ".com",
			wantErr:   true,
			errString: "domain exceeds maximum length of 255 characters",
		},
		{
			name:    "total at max length (384 chars: 128+1+255)",
			input:   strings.Repeat("a", 128) + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "." + strings.Repeat("e", 59) + ".abc",
			wantErr: false,
		},
		{
			name:      "total exceeds max length - domain limit hit first",
			input:     strings.Repeat("a", 128) + "@" + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "." + strings.Repeat("e", 60) + ".abc",
			wantErr:   true,
			errString: "domain exceeds maximum length of 255 characters",
		},
		{
			name:    "address with plus detail within limits",
			input:   "user+detail@example.com",
			wantErr: false,
		},
		{
			name:      "address with plus detail exceeding local part limit",
			input:     strings.Repeat("a", 124) + "+detail@example.com",
			wantErr:   true,
			errString: "local part exceeds maximum length of 128 characters",
		},
		{
			name:    "empty address",
			input:   "",
			wantErr: true,
		},
		{
			name:    "address with whitespace",
			input:   "user @example.com",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := NewAddress(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAddress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if tt.errString != "" && err != nil && !strings.Contains(err.Error(), tt.errString) {
					t.Errorf("NewAddress() error = %v, want error containing %q", err, tt.errString)
				}
				return
			}

			// For valid addresses, verify the address was created successfully
			if addr.FullAddress() == "" {
				t.Errorf("NewAddress() returned empty FullAddress() for valid input")
			}
		})
	}
}

func TestAddressLengthConstants(t *testing.T) {
	// Verify constants: extended local part limit to accommodate SRS addresses
	if MaxLocalPartLength != 128 {
		t.Errorf("MaxLocalPartLength = %d, want 128", MaxLocalPartLength)
	}
	if MaxDomainLength != 255 {
		t.Errorf("MaxDomainLength = %d, want 255", MaxDomainLength)
	}
	if MaxAddressLength != 384 {
		t.Errorf("MaxAddressLength = %d, want 384", MaxAddressLength)
	}
}
