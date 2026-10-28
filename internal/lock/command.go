package lock

// CommandType names a state transition.
type CommandType string

// Command types, one per state transition.
const (
	CmdCreateLease CommandType = "create_lease"
	CmdRenewLease  CommandType = "renew_lease"
	CmdRevokeLease CommandType = "revoke_lease"
	CmdAcquire     CommandType = "acquire"
	CmdRelease     CommandType = "release"
	CmdExpire      CommandType = "expire"
)

// Command is one raft log entry. Now is stamped by the leader just before it
// proposes the entry, so applying it never reads a clock.
type Command struct {
	Type     CommandType `json:"type"`
	LeaseID  string      `json:"lease_id,omitempty"`
	TTL      int64       `json:"ttl_ms,omitempty"`
	Name     string      `json:"name,omitempty"`
	Token    uint64      `json:"token,omitempty"`
	LeaseIDs []string    `json:"lease_ids,omitempty"`
	Now      int64       `json:"now"`
}

// LeaseResult is the outcome of creating or renewing a lease.
type LeaseResult struct {
	LeaseID string
	TTL     int64
	Created bool
}

// AcquireResult is the outcome of acquiring a lock.
type AcquireResult struct {
	Token uint64
}

// ExpireResult is the outcome of an expire batch.
type ExpireResult struct {
	Expired int
}
