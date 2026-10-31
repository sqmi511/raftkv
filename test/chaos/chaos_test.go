package chaos

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/kv/kvtest"
	"github.com/sqmi511/raftkv/internal/lincheck"
)

var (
	chaosDuration = flag.Duration("chaos.duration", 6*time.Second, "how long each scenario keeps injecting faults")
	chaosSeed     = flag.Int64("chaos.seed", 0, "seed for the fault schedule, workload and network (0 = random). The seed repeats the schedule, not the exact interleaving")
)

const (
	clusterSize = 5
	workers     = 4
	opTimeout   = 5 * time.Second
	absent      = "<absent>"
)

// faults selects what the nemesis may do.
type faults struct{ kill, partition, flaky bool }

func TestChaosNoFaults(t *testing.T)     { run(t, faults{}) }
func TestChaosCrashRestart(t *testing.T) { run(t, faults{kill: true}) }
func TestChaosPartitions(t *testing.T)   { run(t, faults{partition: true}) }
func TestChaosFlakyNetwork(t *testing.T) { run(t, faults{flaky: true}) }
func TestChaosEverything(t *testing.T)   { run(t, faults{kill: true, partition: true, flaky: true}) }

func run(t *testing.T, f faults) {
	if testing.Short() {
		t.Skip("chaos tests are skipped with -short")
	}
	seed := *chaosSeed
	if seed == 0 {
		seed = time.Now().UnixNano() & 0x7fffffff
	}
	t.Logf("seed=%d duration=%v (rerun with -chaos.seed=%d)", seed, *chaosDuration, seed)

	keys := []string{"a", "b", "c", "d"}
	c := kvtest.NewClusterWith(t, clusterSize, kvtest.Options{SnapshotThreshold: 25, Seed: seed})
	c.WaitLeader()

	rec := newRecorder()
	clients := make([]*client.Client, workers)
	for i := range clients {
		clients[i] = newClient(t, c.Addrs())
	}

	ctx, cancel := context.WithTimeout(context.Background(), *chaosDuration)
	defer cancel()

	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker(ctx, i, clients[i], rec, keys, seed+int64(i))
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		nemesis(ctx, t, c, f, seed+1000)
	}()
	wg.Wait()

	// Heal everything, then look at the final state of the world.
	c.ResetFaults()
	for _, a := range c.Addrs() {
		if c.IsDown(a) {
			if err := c.Restart(a); err != nil {
				t.Fatalf("restart %s: %v", a, err)
			}
		}
	}
	c.WaitLeader()

	// A last linearizable read of every key joins the history: a write that
	// was acknowledged but lost shows up here.
	final := newClient(t, c.Addrs())
	want := make(map[string]string, len(keys))
	for _, key := range keys {
		call := rec.now()
		rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
		v, found, err := final.Get(rctx, key)
		rcancel()
		if err != nil {
			t.Fatalf("final read of %q failed: %v\n%s", key, err, c.Dump())
		}
		rec.add(lincheck.Op{ClientID: workers, Kind: lincheck.Get, Key: key,
			Found: found, Got: string(v), Call: call, Return: rec.now()})
		want[key] = encode(found, v)
	}

	checkConverged(t, c, keys, want)

	ops := rec.snapshot()
	var unknown int
	for _, op := range ops {
		if op.Unknown {
			unknown++
		}
	}
	t.Logf("recorded %d operations, %d with unknown outcome", len(ops), unknown)

	res := lincheck.Check(ops)
	switch {
	case res.Err != nil:
		t.Fatalf("the linearizability check did not finish: %v\nlower -chaos.duration or rerun with another seed", res.Err)
	case !res.OK:
		t.Fatalf("history is NOT linearizable (key %q, seed %d). Operations on that key:\n%s",
			res.Key, seed, clip(lincheck.Format(ops, res.Key), 200))
	}
}

// ---- workload ------------------------------------------------------------------

func newClient(t *testing.T, addrs []string) *client.Client {
	t.Helper()
	cl, err := client.New(client.Options{
		Addrs:          addrs,
		AttemptTimeout: 600 * time.Millisecond,
		MaxAttempts:    8,
		BackoffMin:     20 * time.Millisecond,
		BackoffMax:     250 * time.Millisecond,
		// Reconnect quickly after a node comes back.
		DialOptions: []grpc.DialOption{grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.5,
				Jitter:     0.2,
				MaxDelay:   500 * time.Millisecond,
			},
			MinConnectTimeout: 500 * time.Millisecond,
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

type recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []lincheck.Op
}

func newRecorder() *recorder { return &recorder{start: time.Now()} }

// now returns nanoseconds since the start (monotonic).
func (r *recorder) now() int64 { return int64(time.Since(r.start)) + 1 }

func (r *recorder) add(op lincheck.Op) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *recorder) snapshot() []lincheck.Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]lincheck.Op(nil), r.ops...)
}

// worker issues random operations until ctx ends and records each one. A
// failed Set or Delete is recorded as "unknown outcome": it may still have
// been applied. A failed Get has no effect and is recorded as unknown too
// (the checker drops it).
func worker(ctx context.Context, id int, cl *client.Client, rec *recorder, keys []string, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	for seq := 0; ctx.Err() == nil; seq++ {
		key := keys[rng.Intn(len(keys))]
		octx, cancel := context.WithTimeout(context.Background(), opTimeout)

		op := lincheck.Op{ClientID: id, Key: key}
		op.Call = rec.now()
		switch p := rng.Intn(10); {
		case p < 4:
			op.Kind = lincheck.Set
			op.Value = fmt.Sprintf("c%d-%d", id, seq) // unique, so reads identify their writer
			op.Unknown = cl.Set(octx, key, []byte(op.Value), 0) != nil
		case p < 6:
			op.Kind = lincheck.Delete
			deleted, err := cl.Delete(octx, key)
			op.Found, op.Unknown = deleted, err != nil
		default:
			op.Kind = lincheck.Get
			v, found, err := cl.Get(octx, key)
			op.Found, op.Got, op.Unknown = found, string(v), err != nil
		}
		op.Return = rec.now()
		cancel()
		rec.add(op)

		time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)
	}
}

// ---- nemesis -------------------------------------------------------------------

// nemesis injects faults every 300-900ms until ctx ends. It never removes
// more than a minority of the nodes at once (killed + cut off <= 2 of 5), so
// a majority always exists and the workload can make progress.
func nemesis(ctx context.Context, t *testing.T, c *kvtest.Cluster, f faults, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	addrs := c.Addrs()
	maxLoss := (len(addrs) - 1) / 2
	start := time.Now()
	logf := func(format string, args ...any) {
		t.Logf("%6s  %s", time.Since(start).Round(10*time.Millisecond), fmt.Sprintf(format, args...))
	}

	var killed, cut []string
	flaky := false

	healthy := func() []string {
		var out []string
		for _, a := range addrs {
			if !contains(killed, a) && !contains(cut, a) {
				out = append(out, a)
			}
		}
		return out
	}

	var actions []func()
	if f.kill {
		actions = append(actions, func() {
			if len(killed) > 0 && (len(killed)+len(cut) >= maxLoss || rng.Intn(2) == 0) {
				addr := killed[rng.Intn(len(killed))]
				if err := c.Restart(addr); err != nil {
					t.Errorf("restart %s: %v", addr, err)
					return
				}
				killed = without(killed, addr)
				logf("restart %s", addr)
				return
			}
			if len(killed)+len(cut) < maxLoss {
				cands := healthy()
				addr := cands[rng.Intn(len(cands))]
				c.Stop(addr)
				killed = append(killed, addr)
				logf("kill %s", addr)
			}
		})
	}
	if f.partition {
		actions = append(actions, func() {
			if len(cut) > 0 {
				c.HealPartition()
				logf("heal partition (was cut off: %v)", cut)
				cut = nil
				return
			}
			room := maxLoss - len(killed)
			if room < 1 {
				return
			}
			cands := healthy()
			rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
			cut = append([]string(nil), cands[:1+rng.Intn(room)]...)
			c.Partition(cut)
			logf("partition: cut off %v", cut)
		})
	}
	if f.flaky {
		actions = append(actions, func() {
			if flaky {
				c.SetDropRate(0)
				c.SetDelay(0, 0)
				flaky = false
				logf("network back to normal")
				return
			}
			drop := 0.05 + 0.10*rng.Float64()
			maxDelay := time.Duration(5+rng.Intn(20)) * time.Millisecond
			c.SetDropRate(drop)
			c.SetDelay(time.Millisecond, maxDelay)
			flaky = true
			logf("flaky network: drop %.0f%%, delay up to %v", drop*100, maxDelay)
		})
	}

	if len(actions) == 0 {
		<-ctx.Done()
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(300+rng.Intn(600)) * time.Millisecond):
		}
		actions[rng.Intn(len(actions))]()
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func without(xs []string, x string) []string {
	var out []string
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

// ---- checks ----------------------------------------------------------------------

func encode(found bool, v []byte) string {
	if !found {
		return absent
	}
	return "=" + string(v)
}

// checkConverged waits until every replica holds exactly the values the final
// linearizable read returned.
func checkConverged(t *testing.T, c *kvtest.Cluster, keys []string, want map[string]string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var bad string
		for _, a := range c.Addrs() {
			for _, k := range keys {
				v, ok := c.Service(a).StaleGet(k)
				if got := encode(ok, v); got != want[k] {
					bad = fmt.Sprintf("node %s holds %s=%s, the cluster's answer is %s", a, k, got, want[k])
				}
			}
		}
		if bad == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replicas did not converge: %s\n%s", bad, c.Dump())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func clip(s string, maxLines int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= maxLines {
		return s
	}
	return strings.Join(lines[:maxLines], "\n") + fmt.Sprintf("\n... %d more lines", len(lines)-maxLines)
}
