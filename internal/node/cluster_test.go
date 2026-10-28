package node_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nemaniabhiram/janus/internal/api"
	"github.com/nemaniabhiram/janus/internal/node"
)

const (
	clusterSize  = 3
	raftTimeout  = 200 * time.Millisecond
	waitTimeout  = 10 * time.Second
	pollInterval = 10 * time.Millisecond
	testTTL      = 5_000
)

// fakeClock is the only stubbed boundary besides the network. Each node gets
// its own, so tests can skew them.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// swapHandler lets a test replace a node's http handler while it serves.
type swapHandler struct {
	mu sync.RWMutex
	h  http.Handler
}

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	h := s.h
	s.mu.RUnlock()
	h.ServeHTTP(w, r)
}

func (s *swapHandler) set(h http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h = h
}

type member struct {
	id      string
	store   *raft.InmemStore
	snaps   *raft.InmemSnapshotStore
	trans   *raft.InmemTransport
	clock   *fakeClock
	handler *swapHandler
	srv     *httptest.Server
	node    *node.Node
	down    bool
}

type cluster struct {
	t       *testing.T
	members []*member
	peers   []node.Peer
}

// newCluster starts an in-memory 3-node cluster. Raft runs over inmem
// transports, http over real httptest servers, so forwarding is real http.
func newCluster(t *testing.T) *cluster {
	t.Helper()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &cluster{t: t}
	for i := range clusterSize {
		m := &member{
			id:      fmt.Sprintf("n%d", i+1),
			store:   raft.NewInmemStore(),
			snaps:   raft.NewInmemSnapshotStore(),
			clock:   &fakeClock{now: start},
			handler: &swapHandler{h: http.NotFoundHandler()},
		}
		m.srv = httptest.NewServer(m.handler)
		t.Cleanup(m.srv.Close)
		c.members = append(c.members, m)
		c.peers = append(c.peers, node.Peer{ID: m.id, RaftAddr: m.id, HTTPURL: m.srv.URL})
	}
	for _, m := range c.members {
		c.start(m)
	}
	t.Cleanup(func() {
		for _, m := range c.members {
			if !m.down {
				_ = m.node.Shutdown()
			}
		}
	})
	return c
}

// start runs a node on the member's stores, so a restart replays its log.
func (c *cluster) start(m *member) {
	c.t.Helper()
	_, m.trans = raft.NewInmemTransport(raft.ServerAddress(m.id))
	for _, o := range c.members {
		if o == m || o.trans == nil {
			continue
		}
		m.trans.Connect(raft.ServerAddress(o.id), o.trans)
		o.trans.Connect(raft.ServerAddress(m.id), m.trans)
	}
	cfg := raft.DefaultConfig()
	cfg.HeartbeatTimeout = raftTimeout
	cfg.ElectionTimeout = raftTimeout
	cfg.LeaderLeaseTimeout = raftTimeout
	cfg.CommitTimeout = 5 * time.Millisecond
	n, err := node.New(node.Options{
		ID:            m.id,
		Peers:         c.peers,
		RaftConfig:    cfg,
		LogStore:      m.store,
		StableStore:   m.store,
		SnapshotStore: m.snaps,
		Transport:     m.trans,
		Clock:         m.clock.Now,
		Logger:        slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.t.Fatalf("start %s: %v", m.id, err)
	}
	m.node = n
	m.down = false
	m.handler.set(api.New(n, slog.New(slog.DiscardHandler)))
}

// kill stops a member's raft and cuts it off, like a crashed process.
func (c *cluster) kill(m *member) {
	c.t.Helper()
	c.isolate(m)
	if err := m.node.Shutdown(); err != nil {
		c.t.Fatalf("shut down %s: %v", m.id, err)
	}
	m.down = true
}

// isolate cuts a member off from every peer in both directions.
func (c *cluster) isolate(m *member) {
	m.trans.DisconnectAll()
	for _, o := range c.members {
		if o != m {
			o.trans.Disconnect(raft.ServerAddress(m.id))
		}
	}
}

// leader waits until exactly one live member is a ready leader.
func (c *cluster) leader() *member {
	c.t.Helper()
	var found *member
	waitFor(c.t, "a ready leader", func() bool {
		for _, m := range c.members {
			if !m.down && node.Ready(m.node) {
				found = m
				return true
			}
		}
		return false
	})
	return found
}

// follower returns a live non-leader that knows who the leader is.
func (c *cluster) follower(leader *member) *member {
	c.t.Helper()
	var found *member
	waitFor(c.t, "a follower that knows the leader", func() bool {
		for _, m := range c.members {
			if m != leader && !m.down && m.node.Status().LeaderID == leader.id {
				found = m
				return true
			}
		}
		return false
	})
	return found
}

func (c *cluster) advanceClocks(d time.Duration) {
	for _, m := range c.members {
		m.clock.Advance(d)
	}
}

type response struct {
	status int
	header http.Header
	body   map[string]any
}

func (r response) code() string {
	e, _ := r.body["error"].(map[string]any)
	s, _ := e["code"].(string)
	return s
}

func call(t *testing.T, m *member, method, path string, body any) response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, m.srv.URL+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", method, path, m.id, err)
	}
	defer func() { _ = res.Body.Close() }()
	out := response{status: res.StatusCode, header: res.Header}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			t.Fatalf("%s %s on %s: body is not json: %q", method, path, m.id, raw)
		}
	}
	return out
}

func createLease(t *testing.T, m *member, id string) {
	t.Helper()
	res := call(t, m, http.MethodPost, "/v1/leases", map[string]any{"lease_id": id, "ttl_ms": testTTL})
	if res.status != http.StatusCreated {
		t.Fatalf("create lease on %s: status %d, body %v", m.id, res.status, res.body)
	}
}

func acquire(t *testing.T, m *member, name, leaseID string) response {
	t.Helper()
	return call(t, m, http.MethodPut, "/v1/locks/"+name, map[string]any{"lease_id": leaseID})
}

func token(t *testing.T, res response) uint64 {
	t.Helper()
	if res.status != http.StatusOK {
		t.Fatalf("acquire: status %d, body %v", res.status, res.body)
	}
	return uint64(res.body["token"].(float64))
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(pollInterval)
	}
}

const (
	leaseA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	leaseB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestFollowerForwardsWriteToLeader(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := c.follower(leader)

	createLease(t, follower, leaseA)

	res := call(t, leader, http.MethodPost, "/v1/leases/"+leaseA+"/renew", nil)
	if res.status != http.StatusOK {
		t.Fatalf("renew on leader: status %d, body %v", res.status, res.body)
	}
}

func TestForwardToLeaderThatDropsConnectionIsOutcomeUnknown(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := c.follower(leader)
	leader.handler.set(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))

	res := call(t, follower, http.MethodPost, "/v1/leases", map[string]any{"lease_id": leaseA, "ttl_ms": testTTL})

	if res.status != http.StatusServiceUnavailable || res.code() != "outcome_unknown" {
		t.Fatalf("status %d, body %v, want 503 outcome_unknown", res.status, res.body)
	}
}

func TestFailoverKeepsHeldLockAndIssuesHigherToken(t *testing.T) {
	c := newCluster(t)
	old := c.leader()
	createLease(t, old, leaseA)
	held := token(t, acquire(t, old, "build", leaseA))

	c.advanceClocks(4 * time.Second)
	c.kill(old)
	// Lease A's deadline passes during the election, when no client can renew.
	c.advanceClocks(2 * time.Second)
	leader := c.leader()

	createLease(t, leader, leaseB)
	if res := acquire(t, leader, "build", leaseB); res.code() != "lock_held" {
		t.Fatalf("acquire of held lock after failover: status %d, body %v, want lock_held", res.status, res.body)
	}
	if next := token(t, acquire(t, leader, "deploy", leaseB)); next <= held {
		t.Errorf("token after failover = %d, want greater than %d", next, held)
	}
}

func TestCrashedClientLeaseIsSweptAndLockFreed(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	createLease(t, leader, leaseA)
	token(t, acquire(t, leader, "build", leaseA))

	c.advanceClocks(6 * time.Second)

	waitFor(t, "the sweep to reclaim lease A and its lock", func() bool {
		st := leader.node.Status()
		return st.Leases == 0 && st.Locks == 0
	})
	createLease(t, leader, leaseB)
	token(t, acquire(t, leader, "build", leaseB))
}
