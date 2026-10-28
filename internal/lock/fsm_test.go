package lock

import (
	"bytes"
	"testing"
)

func TestSnapshotRestoreIsByteIdentical(t *testing.T) {
	f := NewFSM()
	for i, cmd := range []Command{
		create(leaseA, ttl, 1_000),
		create(leaseB, ttl, 2_000),
		acquire("build", leaseA, 3_000),
	} {
		if _, err := f.Apply(uint64(i+1), cmd); err != nil {
			t.Fatalf("apply %s: %v", cmd.Type, err)
		}
	}
	before, err := f.Encode()
	if err != nil {
		t.Fatal(err)
	}

	restored := NewFSM()
	if err := restored.Restore(before); err != nil {
		t.Fatal(err)
	}
	after, err := restored.Encode()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, after) {
		t.Errorf("restored state differs:\nbefore %s\nafter  %s", before, after)
	}
}
