package lock

import "sync"

// FSM guards a State for concurrent use. Raft applies entries one at a time,
// but http handlers and the expiry sweep read state from other goroutines.
type FSM struct {
	mu    sync.RWMutex
	state *State
}

// NewFSM returns an FSM holding an empty state.
func NewFSM() *FSM {
	return &FSM{state: NewState()}
}

// Apply runs cmd, committed at index, under the write lock.
func (f *FSM) Apply(index uint64, cmd Command) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state.Apply(index, cmd)
}
