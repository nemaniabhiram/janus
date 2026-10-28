package node

// Ready exposes readiness to the cluster tests in package node_test.
func Ready(n *Node) bool { return n.ready() }

// Barrier waits until every committed entry is applied on n.
func Barrier(n *Node) error { return n.raft.Barrier(applyTimeout).Error() }

// EncodedState returns n's state machine, encoded for comparison.
func EncodedState(n *Node) ([]byte, error) { return n.fsm.Encode() }
