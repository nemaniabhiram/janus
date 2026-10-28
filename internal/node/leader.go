package node

import (
	"context"
	"time"

	"github.com/nemaniabhiram/janus/internal/lock"
)

const takeOverRetry = 100 * time.Millisecond

// watchLeadership follows raft's leadership changes. hashicorp/raft reports a
// node as leader before its fsm has applied the previous term's entries, so
// leadership alone never opens the gate.
func (n *Node) watchLeadership() {
	defer n.wg.Done()
	stopTerm := context.CancelFunc(func() {})
	defer func() { stopTerm() }()
	for {
		select {
		case <-n.done:
			return
		case isLeader := <-n.leaderCh:
			stopTerm()
			n.gate.close()
			term := n.raft.CurrentTerm()
			if !isLeader {
				n.log.Info("leadership lost", "term", term)
				continue
			}
			n.log.Info("leadership gained", "term", term)
			ctx, cancel := context.WithCancel(context.Background())
			stopTerm = cancel
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				if n.takeOver(ctx, term) {
					n.sweep(ctx)
				}
			}()
		}
	}
}

// takeOver catches up and promotes, then opens the gate for term. It reports
// whether the gate opened. Without the
// barrier a read could see state from before it. Without waiting for promote,
// a write stamped after the election could evict leases promote would protect.
func (n *Node) takeOver(ctx context.Context, term uint64) bool {
	for {
		res, err := n.catchUpAndPromote()
		if err == nil {
			if !n.gate.open(ctx, term) {
				return false
			}
			n.log.Info("promote applied", "term", term, "leases_reset", res.Reset, "leases_dropped", res.Dropped)
			return true
		}
		n.log.Warn("take over failed, retrying", "term", term, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(takeOverRetry):
		}
	}
}

func (n *Node) catchUpAndPromote() (lock.PromoteResult, error) {
	if err := n.wait(n.raft.Barrier(applyTimeout)); err != nil {
		return lock.PromoteResult{}, n.raftError("barrier", err)
	}
	v, err := n.propose(lock.Command{Type: lock.CmdPromote})
	if err != nil {
		return lock.PromoteResult{}, err
	}
	return v.(lock.PromoteResult), nil
}
