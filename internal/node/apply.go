package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nemaniabhiram/janus/internal/lock"
)

const applyTimeout = 5 * time.Second

// Errors that tell a client whether its write happened.
var (
	// ErrNoLeader means nothing was appended. Retrying is safe.
	ErrNoLeader = errors.New("no ready leader")
	// ErrOutcomeUnknown means the entry may or may not commit. Every call is
	// idempotent, so retrying is still safe.
	ErrOutcomeUnknown = errors.New("outcome unknown")
)

// Apply stamps cmd with this node's clock and commits it through raft. It
// returns the state machine's result, or its rejection as an error.
func (n *Node) Apply(cmd lock.Command) (any, error) {
	return n.propose(cmd)
}

func (n *Node) propose(cmd lock.Command) (any, error) {
	cmd.Now = n.now()
	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("encode %s command: %w", cmd.Type, err)
	}
	f := n.raft.Apply(b, applyTimeout)
	if err := f.Error(); err != nil {
		return nil, n.raftError(cmd.Type, err)
	}
	res := f.Response().(applyResult)
	return res.value, res.err
}

// raftError maps a raft failure by what it means for the entry.
func (n *Node) raftError(t lock.CommandType, err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrLeadershipTransferInProgress),
		errors.Is(err, raft.ErrEnqueueTimeout):
		return fmt.Errorf("apply %s: %w: %w", t, ErrNoLeader, err)
	case errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrAbortedByRestore),
		errors.Is(err, raft.ErrRaftShutdown):
		return fmt.Errorf("apply %s: %w: %w", t, ErrOutcomeUnknown, err)
	default:
		n.log.Error("raft apply failed", "command", t, "error", err)
		return fmt.Errorf("apply %s: %w", t, err)
	}
}

func (n *Node) now() int64 { return n.clock().UnixMilli() }
