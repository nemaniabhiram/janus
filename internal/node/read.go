package node

import (
	"fmt"

	"github.com/nemaniabhiram/janus/internal/lock"
)

// ReadLock is a linearizable read. The gate guarantees every entry from
// earlier terms is applied, and VerifyLeader guarantees no newer leader exists
// yet, at the cost of one heartbeat round instead of an fsynced log write.
func (n *Node) ReadLock(name string) (lock.View, error) {
	if !n.ready() {
		return lock.View{}, ErrNoLeader
	}
	if err := n.raft.VerifyLeader().Error(); err != nil {
		return lock.View{}, fmt.Errorf("verify leader: %w: %w", ErrNoLeader, err)
	}
	return n.fsm.ReadLock(name, n.now()), nil
}
