package node

import (
	"context"
	"sync"
)

// gate holds traffic on a new leader until it has caught up and promoted.
// Readiness is scoped to a raft term, so a slow goroutine from an old term can
// never open the gate for a later one.
type gate struct {
	mu    sync.Mutex
	term  uint64
	ready bool
}

// open marks the node ready for term, unless ctx was cancelled first.
func (g *gate) open(ctx context.Context, term uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	g.term, g.ready = term, true
	return true
}

func (g *gate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready = false
}

// admit reports whether the node is ready for raft's current term.
func (g *gate) admit(currentTerm uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ready && g.term == currentTerm
}
