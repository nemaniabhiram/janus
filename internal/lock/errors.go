package lock

import "errors"

// Rejections returned as apply results. A rejected command is still a
// committed log entry, it just changes nothing.
var (
	ErrLeaseNotFound  = errors.New("lease not found")
	ErrLeaseExpired   = errors.New("lease expired")
	ErrLeaseExists    = errors.New("lease exists with a different ttl")
	ErrCapacity       = errors.New("capacity exceeded")
	ErrUnknownCommand = errors.New("unknown command type")
)
