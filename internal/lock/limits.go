package lock

// Limits on replicated state. They are checked inside Apply, so every replica
// enforces them identically.
const (
	MinLeaseTTL = 5_000           // ms, longer than an election plus one expiry tick
	MaxLeaseTTL = 10 * 60 * 1_000 // ms, bounds how long a crashed client's locks linger
	MaxLeases   = 100_000
	MaxLocks    = 100_000
)
