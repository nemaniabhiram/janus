package node

import (
	"context"
	"time"

	"github.com/nemaniabhiram/janus/internal/lock"
)

const (
	expiryInterval = 500 * time.Millisecond
	maxExpireBatch = 1_000
)

// sweep reclaims leases nobody touches. Commands evict an expired lease they
// find, so the sweep is garbage collection: it frees memory and capacity, but
// an expired holder never blocks an acquire while it waits.
func (n *Node) sweep(ctx context.Context) {
	t := time.NewTicker(expiryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.expireBatches(ctx)
		}
	}
}

// expireBatches proposes one expire command per batch. A full batch means more
// are waiting, so it scans again right away instead of sleeping.
func (n *Node) expireBatches(ctx context.Context) {
	for ctx.Err() == nil {
		ids := n.fsm.ExpiredLeases(n.now(), maxExpireBatch)
		if len(ids) == 0 {
			return
		}
		v, err := n.propose(lock.Command{Type: lock.CmdExpire, LeaseIDs: ids})
		if err != nil {
			n.log.Warn("expire failed", "error", err)
			return
		}
		n.log.Info("expire applied", "expired", v.(lock.ExpireResult).Expired)
		if len(ids) < maxExpireBatch {
			return
		}
	}
}
