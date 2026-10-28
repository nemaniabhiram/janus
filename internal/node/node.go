// Package node runs one janus server: raft wiring, time stamping, routing to
// the leader, the readiness gate and the expiry sweep. Lock rules live in
// package lock.
package node

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nemaniabhiram/janus/internal/lock"
)

// Peer is one member of the static cluster.
type Peer struct {
	ID       string
	RaftAddr string
	HTTPURL  string // base url of the peer's http api, e.g. http://janus-1:8080
}

// Options configures a node. Stores and transport are passed in so tests can
// run a cluster in memory.
type Options struct {
	ID            string
	Peers         []Peer
	RaftConfig    *raft.Config // nil means raft.DefaultConfig()
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Transport     raft.Transport
	Clock         func() time.Time
	Logger        *slog.Logger
}

// Node is one member of the cluster.
type Node struct {
	id       string
	raft     *raft.Raft
	fsm      *lock.FSM
	httpURLs map[raft.ServerID]*url.URL
	forward  *http.Transport
	clock    func() time.Time
	log      *slog.Logger
}

// New starts raft and, on first start, bootstraps the cluster from the peer
// list. Every node bootstraps with the same list, which hashicorp/raft allows.
func New(opts Options) (*Node, error) {
	httpURLs, err := parseHTTPURLs(opts.Peers)
	if err != nil {
		return nil, err
	}
	n := &Node{
		id:       opts.ID,
		fsm:      lock.NewFSM(),
		httpURLs: httpURLs,
		forward:  newForwardTransport(),
		clock:    opts.Clock,
		log:      opts.Logger.With("node_id", opts.ID),
	}
	servers := make([]raft.Server, 0, len(opts.Peers))
	for _, p := range opts.Peers {
		servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
	}

	cfg := raft.DefaultConfig()
	if opts.RaftConfig != nil {
		c := *opts.RaftConfig
		cfg = &c
	}
	cfg.LocalID = raft.ServerID(opts.ID)
	cfg.Logger = newHCLog(n.log)

	hasState, err := raft.HasExistingState(opts.LogStore, opts.StableStore, opts.SnapshotStore)
	if err != nil {
		return nil, fmt.Errorf("check existing raft state: %w", err)
	}
	r, err := raft.NewRaft(cfg, raftFSM{n.fsm}, opts.LogStore, opts.StableStore, opts.SnapshotStore, opts.Transport)
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	n.raft = r
	if !hasState {
		err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}
	return n, nil
}

// ready reports whether this node can serve requests itself.
func (n *Node) ready() bool {
	return n.raft.State() == raft.Leader
}

// Shutdown stops raft.
func (n *Node) Shutdown() error {
	if err := n.raft.Shutdown().Error(); err != nil {
		return fmt.Errorf("shut down raft: %w", err)
	}
	return nil
}
