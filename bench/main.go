// Command bench measures a running janus cluster and prints every number the
// readme quotes, as a markdown table. Start the cluster with
// docker compose up --build, then run go run ./bench.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nemaniabhiram/janus/client"
	"github.com/nemaniabhiram/janus/internal/lock"
)

const (
	defaultNodes       = "http://localhost:8081,http://localhost:8082,http://localhost:8083"
	containerPrefix    = "janus-"                            // compose names node n1 container janus-1
	benchLeaseTTL      = lock.MaxLeaseTTL * time.Millisecond // outlives a run, so clients need not renew
	callTimeout        = 5 * time.Second
	pollInterval       = 20 * time.Millisecond
	healthyTimeout     = 60 * time.Second
	drainTimeout       = 5 * time.Minute
	expiryBudget       = 2 * time.Minute // time allowed to create the mass expiry leases
	expiryCreators     = 64
	failoverRuns       = 5
	percentile50       = 0.50
	percentile99       = 0.99
	bytesPerMB         = 1 << 20
	capsLeaseTTL       = lock.MaxLeaseTTL
	nodeIDPrefixLength = 1 // strip the "n" from n1
)

type bench struct {
	urls    []string
	clients int
	dur     time.Duration
	hc      *http.Client
	rows    [][3]string
}

func main() {
	nodes := flag.String("nodes", defaultNodes, "comma-separated http urls of the cluster's nodes")
	clients := flag.Int("clients", 64, "concurrent clients for the throughput and contention runs")
	dur := flag.Duration("duration", 30*time.Second, "length of the throughput and contention runs")
	only := flag.String("only", "throughput,contended,failover,expiry,caps", "scenarios to run")
	flag.Parse()

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = *clients
	b := &bench{
		urls:    strings.Split(*nodes, ","),
		clients: *clients,
		dur:     *dur,
		hc:      &http.Client{Transport: tr, Timeout: callTimeout},
	}
	scenarios := map[string]func(context.Context) error{
		"throughput": b.throughput,
		"contended":  b.contended,
		"failover":   b.failover,
		"expiry":     b.massExpiry,
		"caps":       b.caps,
	}
	ctx := context.Background()
	for _, name := range strings.Split(*only, ",") {
		run, ok := scenarios[name]
		if !ok {
			fail(fmt.Errorf("unknown scenario %q", name))
		}
		fmt.Fprintf(os.Stderr, "running %s\n", name)
		if err := run(ctx); err != nil {
			fail(fmt.Errorf("%s: %w", name, err))
		}
	}
	b.print()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "bench:", err)
	os.Exit(1)
}

func (b *bench) row(claim, result, setup string) {
	b.rows = append(b.rows, [3]string{claim, result, setup})
}

func (b *bench) print() {
	cfg := raft.DefaultConfig()
	fmt.Printf("3 nodes in docker compose on one machine, fsync on. raft heartbeat timeout %s, election timeout %s, leader lease timeout %s.\n\n",
		cfg.HeartbeatTimeout, cfg.ElectionTimeout, cfg.LeaderLeaseTimeout)
	fmt.Println("| measurement | result | setup |")
	fmt.Println("|---|---|---|")
	for _, r := range b.rows {
		fmt.Printf("| %s | %s | %s |\n", r[0], r[1], r[2])
	}
}

func (b *bench) node(i int) *client.Client {
	return &client.Client{BaseURL: b.urls[i%len(b.urls)], HTTP: b.hc}
}

// newLease creates a lease through node i.
func (b *bench) newLease(ctx context.Context, i int, ttl time.Duration) (string, error) {
	id, err := client.NewLeaseID()
	if err != nil {
		return "", err
	}
	if _, err := b.node(i).CreateLease(ctx, id, ttl); err != nil {
		return "", fmt.Errorf("create lease: %w", err)
	}
	return id, nil
}

// throughput runs clients that each loop acquire then release on their own
// lock. Clients are spread over every node, so most calls are forwarded.
func (b *bench) throughput(ctx context.Context) error {
	var pairs atomic.Int64
	lat := make([][]time.Duration, b.clients)
	err := b.runClients(ctx, func(ctx context.Context, i int, lease string) error {
		name := fmt.Sprintf("bench-%d", i)
		c := b.node(i)
		for ctx.Err() == nil {
			start := time.Now()
			token, err := c.Acquire(ctx, name, lease)
			if err != nil {
				return ignoreDone(ctx, err)
			}
			lat[i] = append(lat[i], time.Since(start))
			if err := c.Release(ctx, name, lease, token); err != nil {
				return ignoreDone(ctx, err)
			}
			pairs.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}
	all := slices.Concat(lat...)
	slices.Sort(all)
	setup := fmt.Sprintf("%d clients, %s, spread over %d nodes", b.clients, b.dur, len(b.urls))
	b.row("acquire + release throughput", fmt.Sprintf("%.0f pairs/s", float64(pairs.Load())/b.dur.Seconds()), setup)
	b.row("acquire latency", fmt.Sprintf("p50 %s, p99 %s", ms(pct(all, percentile50)), ms(pct(all, percentile99))), "same run, client-side")
	return nil
}

// contended has every client race for one lock. It counts grants and checks
// that no two clients ever believe they hold the lock at once.
func (b *bench) contended(ctx context.Context) error {
	var grants, holders, maxHolders atomic.Int64
	err := b.runClients(ctx, func(ctx context.Context, i int, lease string) error {
		c := b.node(i)
		for ctx.Err() == nil {
			token, err := c.Acquire(ctx, "bench-contended", lease)
			if errCode(err) == client.CodeLockHeld {
				continue
			}
			if err != nil {
				return ignoreDone(ctx, err)
			}
			grants.Add(1)
			h := holders.Add(1)
			for m := maxHolders.Load(); h > m && !maxHolders.CompareAndSwap(m, h); m = maxHolders.Load() {
			}
			holders.Add(-1)
			if err := c.Release(ctx, "bench-contended", lease, token); err != nil {
				return ignoreDone(ctx, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if maxHolders.Load() > 1 {
		return fmt.Errorf("%d clients held the lock at once", maxHolders.Load())
	}
	b.row("contended acquire", fmt.Sprintf("%.0f grants/s, at most 1 holder", float64(grants.Load())/b.dur.Seconds()),
		fmt.Sprintf("%d clients, one lock, %s", b.clients, b.dur))
	return nil
}

// runClients gives each client a lease, runs fn for b.dur, then revokes the
// leases so later scenarios start from an empty cluster.
func (b *bench) runClients(ctx context.Context, fn func(context.Context, int, string) error) error {
	leases := make([]string, b.clients)
	for i := range leases {
		id, err := b.newLease(ctx, i, benchLeaseTTL)
		if err != nil {
			return err
		}
		leases[i] = id
	}
	runCtx, cancel := context.WithTimeout(ctx, b.dur)
	defer cancel()
	errs := make([]error, b.clients)
	var wg sync.WaitGroup
	for i := range b.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(runCtx, i, leases[i])
		}()
	}
	wg.Wait()
	for i, id := range leases {
		if err := b.node(i).RevokeLease(ctx, id); err != nil {
			return fmt.Errorf("revoke lease: %w", err)
		}
	}
	return errors.Join(errs...)
}

// failover kills the leader's container and times how long until an acquire
// succeeds through another node. That window covers the election, Barrier and
// promote. The container is restarted before the next run.
func (b *bench) failover(ctx context.Context) error {
	var windows []time.Duration
	for run := range failoverRuns {
		leader, err := b.waitHealthy(ctx)
		if err != nil {
			return err
		}
		probe := (leader + 1) % len(b.urls)
		lease, err := b.newLease(ctx, probe, benchLeaseTTL)
		if err != nil {
			return err
		}
		container, err := b.container(ctx, leader)
		if err != nil {
			return err
		}
		if err := docker("kill", container); err != nil {
			return err
		}
		start := time.Now()
		name := fmt.Sprintf("bench-failover-%d", run)
		for {
			if _, err := b.node(probe).Acquire(ctx, name, lease); err == nil {
				break
			}
			if time.Since(start) > healthyTimeout {
				return fmt.Errorf("no acquire succeeded within %s of the kill", healthyTimeout)
			}
			time.Sleep(pollInterval)
		}
		windows = append(windows, time.Since(start))
		if err := b.node(probe).RevokeLease(ctx, lease); err != nil {
			return fmt.Errorf("revoke lease: %w", err)
		}
		if err := docker("start", container); err != nil {
			return err
		}
	}
	if _, err := b.waitHealthy(ctx); err != nil {
		return err
	}
	slices.Sort(windows)
	b.row("failover window", fmt.Sprintf("median %s, max %s", ms(windows[len(windows)/2]), ms(windows[len(windows)-1])),
		fmt.Sprintf("%d leader kills, kill to first successful acquire", failoverRuns))
	return nil
}

// massExpiry creates lock.MaxLeases leases whose deadlines all fall on the same
// instant, abandons them, and times how long the sweep takes to reclaim them.
func (b *bench) massExpiry(ctx context.Context) error {
	if _, err := b.waitHealthy(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(expiryBudget)
	ids := make(chan struct{}, lock.MaxLeases)
	for range lock.MaxLeases {
		ids <- struct{}{}
	}
	close(ids)
	errs := make([]error, expiryCreators)
	var wg sync.WaitGroup
	for w := range expiryCreators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ids {
				ttl := time.Until(deadline)
				if ttl < lock.MinLeaseTTL*time.Millisecond {
					errs[w] = fmt.Errorf("creating %d leases took longer than %s", lock.MaxLeases, expiryBudget)
					return
				}
				if _, err := b.newLease(ctx, w, ttl.Truncate(time.Millisecond)); err != nil {
					errs[w] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	time.Sleep(time.Until(deadline))
	for {
		st, err := b.leaderStatus(ctx)
		if err == nil && st.Leases == 0 {
			break
		}
		if time.Since(deadline) > drainTimeout {
			return fmt.Errorf("leases not reclaimed within %s", drainTimeout)
		}
		time.Sleep(pollInterval)
	}
	b.row("mass expiry drain", fmt.Sprintf("%d leases reclaimed %s after their shared deadline", lock.MaxLeases, ms(time.Since(deadline))),
		"one deadline for every lease, no client touches them")
	return nil
}

// caps fills a state machine to both caps in-process and measures what a node
// pays there: snapshot size and encode time, and one expiry scan.
func (b *bench) caps(context.Context) error {
	f := lock.NewFSM()
	var index uint64
	apply := func(cmd lock.Command) error {
		index++
		_, err := f.Apply(index, cmd)
		return err
	}
	for i := range lock.MaxLeases {
		id := fmt.Sprintf("%032x", i)
		if err := apply(lock.Command{Type: lock.CmdCreateLease, LeaseID: id, TTL: capsLeaseTTL}); err != nil {
			return err
		}
		if i < lock.MaxLocks {
			if err := apply(lock.Command{Type: lock.CmdAcquire, Name: fmt.Sprintf("lock-%d", i), LeaseID: id}); err != nil {
				return err
			}
		}
	}
	start := time.Now()
	snap, err := f.Encode()
	if err != nil {
		return err
	}
	encode := time.Since(start)
	start = time.Now()
	f.ExpiredLeases(0, lock.MaxLeases)
	scan := time.Since(start)
	setup := fmt.Sprintf("%d leases, %d locks, in-process", lock.MaxLeases, lock.MaxLocks)
	b.row("snapshot at the caps", fmt.Sprintf("%.1f MB, encoded in %s", float64(len(snap))/bytesPerMB, ms(encode)), setup)
	b.row("expiry scan at the caps", ms(scan), setup+", nothing expired so every lease is checked")
	return nil
}

// waitHealthy waits until every node is up, agrees on one leader and has
// applied the same index. It returns the leader's position in b.urls.
func (b *bench) waitHealthy(ctx context.Context) (int, error) {
	deadline := time.Now().Add(healthyTimeout)
	for time.Now().Before(deadline) {
		if leader, ok := b.healthy(ctx); ok {
			return leader, nil
		}
		time.Sleep(pollInterval)
	}
	return 0, fmt.Errorf("cluster not healthy within %s", healthyTimeout)
}

func (b *bench) healthy(ctx context.Context) (int, bool) {
	leader := -1
	var leaderID string
	var applied uint64
	for i := range b.urls {
		st, err := b.node(i).Status(ctx)
		if err != nil || st.LeaderID == "" || (leaderID != "" && st.LeaderID != leaderID) || (i > 0 && st.AppliedIndex != applied) {
			return 0, false
		}
		leaderID, applied = st.LeaderID, st.AppliedIndex
		if st.NodeID == st.LeaderID {
			leader = i
		}
	}
	return leader, leader >= 0
}

func (b *bench) leaderStatus(ctx context.Context) (client.Status, error) {
	for i := range b.urls {
		st, err := b.node(i).Status(ctx)
		if err == nil && st.NodeID == st.LeaderID {
			return st, nil
		}
	}
	return client.Status{}, errors.New("no leader")
}

// container maps the node at position i to its compose container name.
func (b *bench) container(ctx context.Context, i int) (string, error) {
	st, err := b.node(i).Status(ctx)
	if err != nil {
		return "", err
	}
	return containerPrefix + st.NodeID[nodeIDPrefixLength:], nil
}

func docker(args ...string) error {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return nil
}

func errCode(err error) string {
	var e *client.APIError
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// ignoreDone drops the error from a call cut short by the end of the run.
func ignoreDone(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func ms(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2f s", d.Seconds())
	}
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}
