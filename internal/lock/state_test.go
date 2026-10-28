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

func acquire(name, leaseID string, now int64) Command {
	return Command{Type: CmdAcquire, Name: name, LeaseID: leaseID, Now: now}
}

func TestAcquireFreeLockUsesEntryIndexAsToken(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))

	res := apply(t, s, 42, acquire("build", leaseA, 1_000)).(AcquireResult)

	if res.Token != 42 {
		t.Errorf("token = %d, want 42", res.Token)
	}
}

func TestAcquireOfLockHeldByLiveLeaseIsRefused(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, create(leaseB, ttl, 1_000))
	apply(t, s, 3, acquire("build", leaseA, 1_000))

	_, err := s.Apply(4, acquire("build", leaseB, 2_000))

	if !errors.Is(err, ErrLockHeld) {
		t.Fatalf("err = %v, want ErrLockHeld", err)
	}
	if got := s.Locks["build"].LeaseID; got != leaseA {
		t.Errorf("holder = %s, want lease A", got)
	}
}

func TestAcquireEvictsHolderPastDeadline(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, create(leaseB, 2*ttl, 1_000))
	apply(t, s, 3, acquire("build", leaseA, 1_000))

	apply(t, s, 4, acquire("build", leaseB, 7_000))

	if got := s.Locks["build"].LeaseID; got != leaseB {
		t.Errorf("holder = %s, want lease B", got)
	}
	if _, ok := s.Leases[leaseA]; ok {
		t.Error("expired holder was not evicted")
	}
}

func TestReentrantAcquireReturnsExistingToken(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, acquire("build", leaseA, 1_000))

	res := apply(t, s, 3, acquire("build", leaseA, 2_000)).(AcquireResult)

	if res.Token != 2 || s.Locks["build"].Token != 2 {
		t.Errorf("token = %d, stored %d, want 2", res.Token, s.Locks["build"].Token)
	}
}

func TestAcquireWithLeaseAtDeadlineIsRefused(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))

	_, err := s.Apply(2, acquire("build", leaseA, 6_000))

	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("err = %v, want ErrLeaseExpired", err)
	}
}

func TestReleaseWithStaleTokenKeepsNewerGrant(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, acquire("build", leaseA, 1_000))
	apply(t, s, 3, Command{Type: CmdRelease, Name: "build", LeaseID: leaseA, Token: 2, Now: 1_000})
	apply(t, s, 4, acquire("build", leaseA, 1_000))

	_, err := s.Apply(5, Command{Type: CmdRelease, Name: "build", LeaseID: leaseA, Token: 2, Now: 1_000})

	if !errors.Is(err, ErrNotHeld) {
		t.Fatalf("err = %v, want ErrNotHeld", err)
	}
	if got := s.Locks["build"]; got == nil || got.Token != 4 {
		t.Errorf("lock = %+v, want the grant with token 4", got)
	}
}

func expire(now int64, ids ...string) Command {
	return Command{Type: CmdExpire, LeaseIDs: ids, Now: now}
}

func TestExpireSkipsLeaseRenewedAfterScan(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, Command{Type: CmdRenewLease, LeaseID: leaseA, Now: 5_500})

	apply(t, s, 3, expire(6_000, leaseA))

	if _, ok := s.Leases[leaseA]; !ok {
		t.Error("renewed lease was expired")
	}
}

func TestExpireReleasesAllLocksOfTheLeaseOnly(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, create(leaseB, 2*ttl, 1_000))
	apply(t, s, 3, acquire("x", leaseA, 1_000))
	apply(t, s, 4, acquire("y", leaseA, 1_000))
	apply(t, s, 5, acquire("z", leaseB, 1_000))

	apply(t, s, 6, expire(7_000, leaseA))

	if _, ok := s.Locks["x"]; ok {
		t.Error("lock x survived its lease")
	}
	if _, ok := s.Locks["y"]; ok {
		t.Error("lock y survived its lease")
	}
	if _, ok := s.Locks["z"]; !ok {
		t.Error("lock z of another lease was released")
	}
}

func TestExpireNeverReleasesLockTheLeaseNoLongerHolds(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, create(leaseB, 2*ttl, 1_000))
	apply(t, s, 3, acquire("x", leaseA, 1_000))
	apply(t, s, 4, Command{Type: CmdRelease, Name: "x", LeaseID: leaseA, Token: 3, Now: 1_000})
	apply(t, s, 5, acquire("x", leaseB, 1_000))

	apply(t, s, 6, expire(7_000, leaseA))

	if got := s.Locks["x"]; got == nil || got.LeaseID != leaseB {
		t.Errorf("lock x = %+v, want held by lease B", got)
	}
}

func TestPromoteResetsLeaseAliveAtLastAppliedTime(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, create(leaseB, ttl, 5_000))

	apply(t, s, 3, Command{Type: CmdPromote, Now: 8_000})

	if l, ok := s.Leases[leaseA]; !ok || l.ExpiresAt != 13_000 {
		t.Errorf("lease A = %+v, want reset to expire at 13000", l)
	}
}

func TestPromoteDropsLeaseAlreadyDeadAtLastAppliedTime(t *testing.T) {
	s := NewState()
	apply(t, s, 1, create(leaseA, ttl, 1_000))
	apply(t, s, 2, acquire("build", leaseA, 1_000))
	apply(t, s, 3, create(leaseB, ttl, 7_000))

	apply(t, s, 4, Command{Type: CmdPromote, Now: 8_000})

	if _, ok := s.Leases[leaseA]; ok {
		t.Error("dead lease was revived by promote")
	}
	if _, ok := s.Locks["build"]; ok {
		t.Error("dead lease's lock survived promote")
	}
}
