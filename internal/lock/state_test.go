package lock

import (
	"errors"
	"fmt"
	"testing"
)

const (
	leaseA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	leaseB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ttl    = 5_000
)

// apply runs cmd at index and fails the test on an unexpected error.
func apply(t *testing.T, s *State, index uint64, cmd Command) any {
	t.Helper()
	res, err := s.Apply(index, cmd)
	if err != nil {
		t.Fatalf("apply %s at index %d: %v", cmd.Type, index, err)
	}
	return res
}

func create(id string, ttl, now int64) Command {
	return Command{Type: CmdCreateLease, LeaseID: id, TTL: ttl, Now: now}
}

func TestCreateRepeatReturnsExistingLeaseWithoutMovingDeadline(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))

	res := apply(t, s, 2, create(leaseA, ttl, 3_000)).(LeaseResult)

	if res.Created {
		t.Error("repeat create reported a new lease")
	}
	if got := s.Leases[leaseA].ExpiresAt; got != 6_000 {
		t.Errorf("expiresAt = %d, want 6000", got)
	}
}

func TestCreateOverExpiredUnsweptIDMakesFreshLease(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))

	res := apply(t, s, 2, create(leaseA, 2*ttl, 7_000)).(LeaseResult)

	if !res.Created {
		t.Error("create over an expired id did not make a new lease")
	}
	if got := s.Leases[leaseA].ExpiresAt; got != 17_000 {
		t.Errorf("expiresAt = %d, want 17000", got)
	}
}

func TestRenewOfExpiredLeaseIsRefused(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))

	_, err := s.Apply(2, Command{Type: CmdRenewLease, LeaseID: leaseA, Now: 7_000})

	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("err = %v, want ErrLeaseExpired", err)
	}
	if _, ok := s.Leases[leaseA]; ok {
		t.Error("expired lease still present after renew")
	}
}

func TestCommandStampedBeforeStateNowIsJudgedAtStateNow(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 7_000))

	apply(t, s, 2, create(leaseB, ttl, 2_000))

	if got := s.Leases[leaseB].ExpiresAt; got != 12_000 {
		t.Errorf("expiresAt = %d, want 12000", got)
	}
}

func TestCreateBeyondMaxLeasesIsRefused(t *testing.T) {
	s := NewState()
	for i := range MaxLeases {
		apply(t, s, uint64(i+1), create(fmt.Sprintf("%032x", i), ttl, 1_000))
	}

	_, err := s.Apply(MaxLeases+1, create(leaseA, ttl, 1_000))

	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("err = %v, want ErrCapacity", err)
	}
}
