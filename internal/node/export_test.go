package node

// Ready exposes readiness to the cluster tests in package node_test.
func Ready(n *Node) bool { return n.ready() }
