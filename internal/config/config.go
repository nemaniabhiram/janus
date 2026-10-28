// Package config reads and validates node configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Ports every node listens on. In docker each node is its own host, so the
// same ports work for every peer.
const (
	RaftPort = 7000
	HTTPPort = 8080
)

// Environment variable names.
const (
	EnvNodeID  = "JANUS_NODE_ID"
	EnvPeers   = "JANUS_PEERS"
	EnvDataDir = "JANUS_DATA_DIR"
)

// Peer is one member of the static cluster.
type Peer struct {
	ID   string
	Host string
}

// RaftAddr is the peer's raft transport address.
func (p Peer) RaftAddr() string { return net.JoinHostPort(p.Host, strconv.Itoa(RaftPort)) }

// HTTPAddr is the peer's http api address.
func (p Peer) HTTPAddr() string { return net.JoinHostPort(p.Host, strconv.Itoa(HTTPPort)) }

// Config is the validated configuration of one node.
type Config struct {
	NodeID  string
	DataDir string
	Peers   []Peer
}

// Self returns the peer entry for this node.
func (c Config) Self() Peer {
	for _, p := range c.Peers {
		if p.ID == c.NodeID {
			return p
		}
	}
	return Peer{}
}

// Load reads the configuration through getenv and fails on any missing or
// malformed value, naming the variable at fault.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		NodeID:  strings.TrimSpace(getenv(EnvNodeID)),
		DataDir: strings.TrimSpace(getenv(EnvDataDir)),
	}
	if cfg.NodeID == "" {
		return Config{}, fmt.Errorf("%s: %w", EnvNodeID, errMissing)
	}
	if cfg.DataDir == "" {
		return Config{}, fmt.Errorf("%s: %w", EnvDataDir, errMissing)
	}
	peers, err := parsePeers(getenv(EnvPeers))
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", EnvPeers, err)
	}
	cfg.Peers = peers
	if cfg.Self() == (Peer{}) {
		return Config{}, fmt.Errorf("%s: node %q is not in the peer list", EnvPeers, cfg.NodeID)
	}
	return cfg, nil
}

var errMissing = errors.New("must be set")

func parsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errMissing
	}
	seen := make(map[string]bool)
	var peers []Peer
	for _, entry := range strings.Split(raw, ",") {
		id, host, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok {
			return nil, fmt.Errorf("entry %q has no \"=\"", entry)
		}
		if id == "" || host == "" {
			return nil, fmt.Errorf("entry %q needs both an id and a host", entry)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate id %q", id)
		}
		seen[id] = true
		peers = append(peers, Peer{ID: id, Host: host})
	}
	return peers, nil
}
