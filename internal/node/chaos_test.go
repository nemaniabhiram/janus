package node_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nemaniabhiram/janus/client"
	"github.com/nemaniabhiram/janus/internal/node"
)

const (
	chaosDuration   = 10 * time.Second
	chaosClients    = 8
	chaosLocks      = 3
	chaosSeedEnv    = "JANUS_CHAOS_SEED"
	chaosNodeSkew   = 300 * time.Millisecond // clock offset between neighbouring nodes
	maxFaultPause   = 800 * time.Millisecond
	maxClockJump    = 3 * time.Second
	settlePolls     = 5
	chaosLeaseTTL   = testTTL * time.Millisecond
	releaseOneIn    = 2
	faultKinds      = 3
	faultKillLeader = 0
	faultPartition  = 1
)

var chaosLockNames = [chaosLocks]string{"alpha", "beta", "gamma"}

// grant is one successful acquire as a client saw it.
type grant struct {
	lock, lease string
	token       uint64
	start, end  time.Time
}

// history records every grant and plays the protected resource: a fenced
// register per lock that keeps the largest token it has seen and rejects
// anything smaller.
type history struct {
	mu       sync.Mutex
	grants   []grant
	register map[string]uint64
	accepted map[string][]uint64
}

func (h *history) record(g grant) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.grants = append(h.grants, g)
}

// write is the resource's fencing check.
func (h *history) write(lock string, token uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if token < h.register[lock] {
		return
	}
	h.register[lock] = token
	h.accepted[lock] = append(h.accepted[lock], token)
}

func TestChaosReplicasConvergeAndTokensStayFenced(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test is skipped with -short")
	}
	seed := chaosSeed(t)
	t.Logf("chaos seed %d (rerun with %s=%d)", seed, chaosSeedEnv, seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	c := newCluster(t)
	for i, m := range c.members {
		m.clock.Advance(time.Duration(i) * chaosNodeSkew)
	}
	c.leader()

	// Pool a connection per client and node, or the clients churn through
	// ephemeral ports.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = chaosClients
	hc := &http.Client{Transport: tr}
	defer tr.CloseIdleConnections()

	h := &history{register: map[string]uint64{}, accepted: map[string][]uint64{}}
	ctx, cancel := context.WithTimeout(context.Background(), chaosDuration)
	defer cancel()
	var wg sync.WaitGroup
	for i := range chaosClients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runChaosClient(ctx, c, h, hc, rand.New(rand.NewPCG(seed, uint64(i))))
		}()
	}
	for ctx.Err() == nil {
		c.injectFault(rng)
		pause(rng)
	}
	wg.Wait()

	states := c.settle()
	for i, s := range states[1:] {
		if !bytes.Equal(states[0], s) {
			t.Errorf("%s state differs from %s:\n%s\n%s", c.members[i+1].id, c.members[0].id, s, states[0])
		}
	}
	checkGrants(t, h)
}

func chaosSeed(t *testing.T) uint64 {
	if s := os.Getenv(chaosSeedEnv); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("%s: %v", chaosSeedEnv, err)
		}
		return v
	}
	return uint64(time.Now().UnixNano())
}

func pause(rng *rand.Rand) {
	time.Sleep(time.Duration(rng.Int64N(int64(maxFaultPause))))
}

// injectFault crashes the leader, partitions a node or jumps every clock. Each
// fault heals before the next, so a majority usually exists.
func (c *cluster) injectFault(rng *rand.Rand) {
	switch rng.IntN(faultKinds) {
	case faultKillLeader:
		for _, m := range c.members {
			if !m.down && node.Ready(m.node) {
				c.kill(m)
				pause(rng)
				c.start(m)
				return
			}
		}
	case faultPartition:
		c.isolate(c.members[rng.IntN(len(c.members))])
		pause(rng)
		c.heal()
	default:
		c.advanceClocks(time.Duration(rng.Int64N(int64(maxClockJump))))
	}
}

// runChaosClient loops through create, renew, acquire, write, read and release
// on a random node each time, starting a new lease whenever its lease is gone.
func runChaosClient(ctx context.Context, c *cluster, h *history, hc *http.Client, rng *rand.Rand) {
	pick := func() *client.Client {
		return &client.Client{BaseURL: c.members[rng.IntN(len(c.members))].srv.URL, HTTP: hc}
	}
	var lease string
	for ctx.Err() == nil {
		if lease == "" {
			id, err := client.NewLeaseID()
			if err != nil {
				return
			}
			if _, err := pick().CreateLease(ctx, id, chaosLeaseTTL); err == nil {
				lease = id
			}
			continue
		}
		if err := pick().RenewLease(ctx, lease); leaseGone(err) {
			lease = ""
			continue
		}
		name := chaosLockNames[rng.IntN(chaosLocks)]
		start := time.Now()
		token, err := pick().Acquire(ctx, name, lease)
		if leaseGone(err) {
			lease = ""
			continue
		}
		if err != nil {
			continue
		}
		h.record(grant{lock: name, lease: lease, token: token, start: start, end: time.Now()})
		h.write(name, token)
		_, _ = pick().Lock(ctx, name)
		if rng.IntN(releaseOneIn) == 0 {
			_ = pick().Release(ctx, name, lease, token)
		}
	}
}

func leaseGone(err error) bool {
	var e *client.APIError
	return errors.As(err, &e) && (e.Code == client.CodeLeaseExpired || e.Code == client.CodeLeaseNotFound)
}

// settle heals the cluster, restarts any node still down and waits until every
// node has applied the same index and holds still. It returns each node's
// encoded state. Clocks only move inside injectFault, so they are frozen now.
func (c *cluster) settle() [][]byte {
	c.t.Helper()
	for _, m := range c.members {
		if m.down {
			c.start(m)
		}
	}
	c.heal()
	if err := node.Barrier(c.leader().node); err != nil {
		c.t.Fatalf("barrier: %v", err)
	}
	var last uint64
	stable := 0
	waitFor(c.t, "every node to apply the same index", func() bool {
		idx := c.members[0].node.Status().AppliedIndex
		for _, m := range c.members[1:] {
			if m.node.Status().AppliedIndex != idx {
				stable = 0
				return false
			}
		}
		if idx == last {
			stable++
		} else {
			last, stable = idx, 0
		}
		return stable >= settlePolls
	})
	states := make([][]byte, len(c.members))
	for i, m := range c.members {
		s, err := node.EncodedState(m.node)
		if err != nil {
			c.t.Fatalf("encode %s: %v", m.id, err)
		}
		states[i] = s
	}
	return states
}

// checkGrants asserts no token went to two different grants, and that a grant
// which finished before another started on the same lock has the smaller
// token. A re-entrant acquire repeats its grant's token, so it is not a new
// grant.
func checkGrants(t *testing.T, h *history) {
	t.Helper()
	first := map[uint64]grant{}
	for _, g := range h.grants {
		prev, ok := first[g.token]
		if !ok {
			first[g.token] = g
			continue
		}
		if prev.lock != g.lock || prev.lease != g.lease {
			t.Errorf("token %d granted twice: %s to %s and %s to %s", g.token, prev.lock, prev.lease, g.lock, g.lease)
		}
		if g.end.Before(prev.end) {
			first[g.token] = g
		}
	}
	for _, a := range first {
		for _, b := range first {
			if a.lock == b.lock && a.end.Before(b.start) && a.token > b.token {
				t.Errorf("lock %s: token %d finished before token %d started", a.lock, a.token, b.token)
			}
		}
	}
	for lock, tokens := range h.accepted {
		for i := 1; i < len(tokens); i++ {
			if tokens[i] < tokens[i-1] {
				t.Errorf("register %s accepted stale token %d after %d", lock, tokens[i], tokens[i-1])
			}
		}
	}
	if len(first) == 0 {
		t.Error("no lock was ever granted")
	}
	t.Logf("%d distinct grants", len(first))
}
