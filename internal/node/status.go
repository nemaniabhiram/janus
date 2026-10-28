package node

import "strings"

// Status is this node's local view of the cluster.
type Status struct {
	NodeID       string
	State        string
	LeaderID     string
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	Leases       int
	Locks        int
}

// Status reports the local view without consulting the leader.
func (n *Node) Status() Status {
	_, leader := n.raft.LeaderWithID()
	leases, locks := n.fsm.Counts()
	return Status{
		NodeID:       n.id,
		State:        strings.ToLower(n.raft.State().String()),
		LeaderID:     string(leader),
		Term:         n.raft.CurrentTerm(),
		CommitIndex:  n.raft.CommitIndex(),
		AppliedIndex: n.raft.AppliedIndex(),
		Leases:       leases,
		Locks:        locks,
	}
}
