package lock

import (
	"encoding/json"
	"fmt"
	"sync"
)

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

// Encode returns the state as json. encoding/json sorts map keys, so replicas
// holding the same state encode to the same bytes. Encoding under the read
// lock also makes it a deep copy, safe to persist while later entries apply.
func (f *FSM) Encode() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := json.Marshal(f.state)
	if err != nil {
		return nil, fmt.Errorf("encode state: %w", err)
	}
	return b, nil
}

// Restore replaces the whole state, Now included, with an encoded one.
func (f *FSM) Restore(b []byte) error {
	s := NewState()
	if err := json.Unmarshal(b, s); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = s
	return nil
}
