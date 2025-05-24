package helpers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// parseSize parses a human-readable size string (e.g., "1gb", "512mb", "1024kb", "2048b")
// and returns the size in bytes as an int64.
// It supports units: b (bytes), kb (kilobytes), mb (megabytes), gb (gigabytes).
// The units are case-insensitive.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("cannot parse empty size string")
	}

	// Regex to capture the numeric part and the unit part.
	// It allows for optional whitespace between number and unit.
	re := regexp.MustCompile(`^(\d+)\s*([kmgtKMGT]?b|[bB])$`)
	matches := re.FindStringSubmatch(s)

	if len(matches) != 3 {
		// Check if it's just a number (implies bytes)
		numOnlyRe := regexp.MustCompile(`^(\d+)$`)
		numMatches := numOnlyRe.FindStringSubmatch(s)
		if len(numMatches) == 2 {
			size, err := strconv.ParseInt(numMatches[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size format: failed to parse number '%s': %w", numMatches[1], err)
			}
			return size, nil // Treat as bytes
		}
		return 0, fmt.Errorf("invalid size format: '%s'. Expected format like '1024b', '5mb', '1gb'", s)
	}

	sizeStr := matches[1]
	unitStr := strings.ToLower(matches[2])

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		// This should ideally not happen if the regex matched a \d+
		return 0, fmt.Errorf("failed to parse size number '%s': %w", sizeStr, err)
	}

	switch unitStr {
	case "b":
		return size, nil
	case "kb":
		return size * 1024, nil
	case "mb":
		return size * 1024 * 1024, nil
	case "gb":
		return size * 1024 * 1024 * 1024, nil
	// Add tb (terabytes) if needed
	// case "tb":
	// 	return size * 1024 * 1024 * 1024 * 1024, nil
	default:
		// This case should also be rare due to the regex, but good for safety.
		return 0, fmt.Errorf("invalid or unsupported size unit: '%s'", unitStr)
	}
}

// ParseDuration extends time.ParseDuration to support "d" for days.
// It specifically handles strings of the format "[+/-]Nd" (e.g., "14d", "-7d", "+5d")
// by converting them to hours (e.g., "14d" becomes "336h", "-7d" becomes "-168h")
// before passing to time.ParseDuration. Other formats are passed directly to
// time.ParseDuration.
func ParseDuration(durationStr string) (time.Duration, error) {
	originalInput := durationStr // Keep original for context in errors

	s := durationStr
	isNegative := false
	if strings.HasPrefix(s, "-") {
		isNegative = true
		s = s[1:] // Process without the sign for now
	} else if strings.HasPrefix(s, "+") {
		s = s[1:] // Process without the sign
	}

	if strings.HasSuffix(s, "d") {
		daysPart := strings.TrimSuffix(s, "d")
		if daysPart == "" {
			return 0, fmt.Errorf("helpers.ParseDuration: invalid duration format, missing day value in '%s'", originalInput)
		}

		days, err := strconv.Atoi(daysPart)
		if err == nil {
			// Successfully parsed an integer number of days.
			// This implies the input string was of the form "[<sign>]<number>d".
			hours := days * 24
			if isNegative {
				hours = -hours
			}
			return time.ParseDuration(fmt.Sprintf("%dh", hours))
		}
		// If strconv.Atoi(daysPart) failed, fall through to standard parsing.
	}

	return time.ParseDuration(originalInput)
}
