package node

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/nemaniabhiram/janus/internal/lock"
)

// raftFSM adapts lock.FSM to hashicorp/raft.
type raftFSM struct {
	fsm *lock.FSM
}

// applyResult carries the state machine's answer back through the raft future.
type applyResult struct {
	value any
	err   error
}

func (f raftFSM) Apply(l *raft.Log) any {
	var cmd lock.Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return applyResult{err: fmt.Errorf("decode command at index %d: %w", l.Index, err)}
	}
	v, err := f.fsm.Apply(l.Index, cmd)
	return applyResult{value: v, err: err}
}

func (f raftFSM) Snapshot() (raft.FSMSnapshot, error) {
	b, err := f.fsm.Encode()
	if err != nil {
		return nil, err
	}
	return snapshot(b), nil
}

func (f raftFSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	return f.fsm.Restore(b)
}

// snapshot is the encoded state, copied at Snapshot time so Persist can run
// while later entries apply.
type snapshot []byte

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}
	return sink.Close()
}

func (snapshot) Release() {}
