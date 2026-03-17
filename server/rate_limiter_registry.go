package server

import (
	"sync"
)

// RateLimiterRegistry provides a global registry for auth rate limiters
// This allows the Admin API to query blocked IPs across all protocol servers
type RateLimiterRegistry struct {
	mu       sync.RWMutex
	limiters map[string]*AuthRateLimiter // key: "protocol:server_name"
}

var globalRateLimiterRegistry = &RateLimiterRegistry{
	limiters: make(map[string]*AuthRateLimiter),
}

// RegisterRateLimiter registers a rate limiter with the global registry
func RegisterRateLimiter(protocol, serverName string, limiter *AuthRateLimiter) {
	if limiter == nil {
		return
	}
	key := protocol + ":" + serverName
	globalRateLimiterRegistry.mu.Lock()
	globalRateLimiterRegistry.limiters[key] = limiter
	globalRateLimiterRegistry.mu.Unlock()
}

// UnregisterRateLimiter removes a rate limiter from the global registry
func UnregisterRateLimiter(protocol, serverName string) {
	key := protocol + ":" + serverName
	globalRateLimiterRegistry.mu.Lock()
	delete(globalRateLimiterRegistry.limiters, key)
	globalRateLimiterRegistry.mu.Unlock()
}

// GetAllBlockedEntries returns all blocked entries from all registered rate limiters
func GetAllBlockedEntries() []BlockedEntry {
	globalRateLimiterRegistry.mu.RLock()
	defer globalRateLimiterRegistry.mu.RUnlock()

	allEntries := make([]BlockedEntry, 0)
	for _, limiter := range globalRateLimiterRegistry.limiters {
		entries := limiter.GetBlockedEntries()
		allEntries = append(allEntries, entries...)
	}
	return allEntries
}

// GetBlockedEntriesByProtocol returns blocked entries for a specific protocol
func GetBlockedEntriesByProtocol(protocol string) []BlockedEntry {
	globalRateLimiterRegistry.mu.RLock()
	defer globalRateLimiterRegistry.mu.RUnlock()

	allEntries := make([]BlockedEntry, 0)
	for key, limiter := range globalRateLimiterRegistry.limiters {
		// Check if key starts with protocol prefix
		if len(key) > len(protocol) && key[:len(protocol)] == protocol && key[len(protocol)] == ':' {
			entries := limiter.GetBlockedEntries()
			allEntries = append(allEntries, entries...)
		}
	}
	return allEntries
}
