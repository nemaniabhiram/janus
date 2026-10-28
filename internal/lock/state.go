// Package lock holds the replicated state machine: leases, locks and fencing
// tokens. It is a pure function of the log. It reads no clock and does no i/o.
package lock

import "fmt"

// State is everything the cluster agrees on.
type State struct {
	Now    int64             `json:"now"` // ms, the largest stamped time applied so far
	Leases map[string]*Lease `json:"leases"`
	Locks  map[string]*Lock  `json:"locks"`
}

// Lease proves a client is alive until ExpiresAt.
type Lease struct {
	ID        string              `json:"id"`
	TTL       int64               `json:"ttl_ms"`
	ExpiresAt int64               `json:"expires_at"`
	Locks     map[string]struct{} `json:"locks"` // names this lease holds
}

// Lock is a named lock owned by a lease.
type Lock struct {
	Name    string `json:"name"`
	LeaseID string `json:"lease_id"`
	Token   uint64 `json:"token"` // raft index of the granting entry
}

// NewState returns an empty state.
func NewState() *State {
	return &State{Leases: make(map[string]*Lease), Locks: make(map[string]*Lock)}
}

// Apply runs cmd, committed at index, against the state. Rejections come back
// as typed errors and leave the state unchanged apart from evictions.
func (s *State) Apply(index uint64, cmd Command) (any, error) {
	// A new leader's clock may be behind the old one. Clamping keeps time
	// inside the state machine from moving backwards.
	s.Now = max(s.Now, cmd.Now)

	switch cmd.Type {
	case CmdCreateLease:
		return s.createLease(cmd.LeaseID, cmd.TTL)
	case CmdRenewLease:
		return s.renewLease(cmd.LeaseID)
	case CmdRevokeLease:
		s.revokeLease(cmd.LeaseID)
		return nil, nil
	case CmdAcquire:
		return s.acquire(index, cmd.Name, cmd.LeaseID)
	case CmdRelease:
		return nil, s.release(cmd.Name, cmd.LeaseID, cmd.Token)
	case CmdExpire:
		return s.expire(cmd.LeaseIDs), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownCommand, cmd.Type)
	}
}

func (s *State) createLease(id string, ttl int64) (LeaseResult, error) {
	l, err := s.liveLease(id)
	if err == nil {
		if l.TTL != ttl {
			return LeaseResult{}, ErrLeaseExists
		}
		return LeaseResult{LeaseID: id, TTL: ttl}, nil
	}
	if len(s.Leases) >= MaxLeases {
		return LeaseResult{}, ErrCapacity
	}
	s.Leases[id] = &Lease{ID: id, TTL: ttl, ExpiresAt: s.Now + ttl, Locks: make(map[string]struct{})}
	return LeaseResult{LeaseID: id, TTL: ttl, Created: true}, nil
}

func (s *State) renewLease(id string) (LeaseResult, error) {
	l, err := s.liveLease(id)
	if err != nil {
		return LeaseResult{}, err
	}
	l.ExpiresAt = s.Now + l.TTL
	return LeaseResult{LeaseID: id, TTL: l.TTL}, nil
}

func (s *State) revokeLease(id string) {
	if l, ok := s.Leases[id]; ok {
		s.dropLease(l)
	}
}

// acquire grants the lock with the entry index as its fencing token. A holder
// past its deadline is evicted first, so an expired lease never blocks.
func (s *State) acquire(index uint64, name, leaseID string) (AcquireResult, error) {
	l, err := s.liveLease(leaseID)
	if err != nil {
		return AcquireResult{}, err
	}
	if held, ok := s.Locks[name]; ok {
		if held.LeaseID == leaseID {
			return AcquireResult{Token: held.Token}, nil
		}
		if _, err := s.liveLease(held.LeaseID); err == nil {
			return AcquireResult{}, ErrLockHeld
		}
	}
	if len(s.Locks) >= MaxLocks {
		return AcquireResult{}, ErrCapacity
	}
	s.Locks[name] = &Lock{Name: name, LeaseID: leaseID, Token: index}
	l.Locks[name] = struct{}{}
	return AcquireResult{Token: index}, nil
}

// release is a conditional delete. Matching the token keeps a late retry of
// an old release from dropping a newer grant held by the same lease.
func (s *State) release(name, leaseID string, token uint64) error {
	held, ok := s.Locks[name]
	if !ok || held.LeaseID != leaseID || held.Token != token {
		return ErrNotHeld
	}
	delete(s.Locks, name)
	delete(s.Leases[leaseID].Locks, name)
	return nil
}

// expire drops each listed lease that is past its deadline. The leader's scan
// and this apply are separate moments, so a lease renewed in between survives.
func (s *State) expire(ids []string) ExpireResult {
	var res ExpireResult
	for _, id := range ids {
		if l, ok := s.Leases[id]; ok && s.Now >= l.ExpiresAt {
			s.dropLease(l)
			res.Expired++
		}
	}
	return res
}

// liveLease returns the lease if it is live at s.Now. A lease past its
// deadline is evicted on the spot, along with its locks.
func (s *State) liveLease(id string) (*Lease, error) {
	l, ok := s.Leases[id]
	if !ok {
		return nil, ErrLeaseNotFound
	}
	if s.Now >= l.ExpiresAt {
		s.dropLease(l)
		return nil, ErrLeaseExpired
	}
	return l, nil
}

// dropLease deletes a lease and every lock it holds. It trusts the lease's
// reverse index, so the cost follows the lease, not the total lock count.
func (s *State) dropLease(l *Lease) {
	for name := range l.Locks {
		delete(s.Locks, name)
	}
	delete(s.Leases, l.ID)
}
