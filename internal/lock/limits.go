package lock

import "regexp"

// Limits on replicated state. They are checked inside Apply, so every replica
// enforces them identically.
const (
	MinLeaseTTL = 5_000           // ms, longer than an election plus one expiry tick
	MaxLeaseTTL = 10 * 60 * 1_000 // ms, bounds how long a crashed client's locks linger
	MaxLeases   = 100_000
	MaxLocks    = 100_000
)

// Input limits the api checks before proposing a command.
const (
	MaxLockNameLen = 128
	leaseIDLen     = 32 // 128 random bits, hex-encoded
)

var lockNamePattern = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// ValidLeaseID reports whether id is 32 lowercase hex characters.
func ValidLeaseID(id string) bool {
	if len(id) != leaseIDLen {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ValidName reports whether name is a legal lock name.
func ValidName(name string) bool {
	return len(name) <= MaxLockNameLen && lockNamePattern.MatchString(name)
}

// ValidTTL reports whether ttl, in ms, is within the lease bounds.
func ValidTTL(ttl int64) bool {
	return ttl >= MinLeaseTTL && ttl <= MaxLeaseTTL
}
