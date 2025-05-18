package helpers

import (
	"strings"
)

// FindCommonPath finds the common path between two paths based on the delimiter.
// This is useful for operations like mailbox renaming where we need to determine
// the common parent path.
//
// Parameters:
// - oldPath: The original path
// - newPath: The new path
// - delimiter: The delimiter used to separate path components (e.g., "." or "/")
//
// Returns:
// - The common path prefix, or an empty string if there is no common path
func FindCommonPath(oldPath, newPath, delimiter string) string {
	// Split the paths into components
	oldParts := strings.Split(oldPath, delimiter)
	newParts := strings.Split(newPath, delimiter)

	// Determine the shorter length to prevent out-of-range errors
	minLen := len(oldParts)
	if len(newParts) < minLen {
		minLen = len(newParts)
	}

	// Iterate to find the common prefix
	commonParts := []string{}
	for i := 0; i < minLen; i++ {
		if oldParts[i] == newParts[i] {
			commonParts = append(commonParts, oldParts[i])
		} else {
			break
		}
	}

	// If there's no common path, return an empty string
	if len(commonParts) == 0 {
		return ""
	}

	// Reconstruct the common path
	commonPath := strings.Join(commonParts, delimiter)
	return commonPath
}
