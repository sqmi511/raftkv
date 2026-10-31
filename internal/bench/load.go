package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sqmi511/raftkv/client"
)

// opTimeout bounds one operation, retries included.
const opTimeout = 10 * time.Second

// ErrInvalidWorkload is wrapped by errors for a malformed Workload.
var ErrInvalidWorkload = errors.New("bench: invalid workload")

// Workload describes one load scenario.
type Workload struct {
	Name         string
	Clients      int           // concurrent clients; each issues one request at a time
	Duration     time.Duration // how long to apply load
	ValueSize    int           // bytes per written value
	ReadFraction float64       // 0 = all writes, 1 = all (linearizable) reads
	Keys         int           // size of the key space
}

func (w Workload) validate() error {
	switch {
	case w.Clients < 1:
		return fmt.Errorf("%w: clients must be at least 1", ErrInvalidWorkload)
	case w.Duration <= 0:
		return fmt.Errorf("%w: duration must be positive", ErrInvalidWorkload)
	case w.ValueSize < 0:
		return fmt.Errorf("%w: value size must not be negative", ErrInvalidWorkload)
	case w.Keys < 1:
		return fmt.Errorf("%w: keys must be at least 1", ErrInvalidWorkload)
	case w.ReadFraction < 0 || w.ReadFraction > 1:
		return fmt.Errorf("%w: read fraction must be within [0, 1]", ErrInvalidWorkload)
	}
	return nil
}

// Result is the outcome of running one Workload.
type Result struct {
	Workload Workload
	Ops      int // successful operations
	Errors   int // failed operations (not included in Latency)
	Elapsed  time.Duration
	Latency  Summary // of the successful operations
}

// OpsPerSec is the rate of successful operations.
func (r Result) OpsPerSec() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Ops) / r.Elapsed.Seconds()
}

func newClient(addrs []string) (*client.Client, error) {
	return client.New(client.Options{Addrs: addrs, MaxAttempts: 20})
}

// Run applies w to the cluster at addrs and reports what it measured.
// Connections, leader discovery and key population happen before the clock
// starts, so they are not part of the result.
func Run(ctx context.Context, addrs []string, w Workload) (Result, error) {
	if err := w.validate(); err != nil {
		return Result{}, err
	}
	value := bytes.Repeat([]byte{'x'}, w.ValueSize)

	clients := make([]*client.Client, 0, w.Clients)
	defer func() {
		for _, cl := range clients {
			_ = cl.Close()
		}
	}()
	for i := 0; i < w.Clients; i++ {
		cl, err := newClient(addrs)
		if err != nil {
			return Result{}, err
		}
		clients = append(clients, cl)
	}

	// Warm up: every client connects and finds the leader.
	for _, cl := range clients {
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, _, err := cl.Get(wctx, "bench-warmup")
		cancel()
		if err != nil {
			return Result{}, fmt.Errorf("bench: warm-up failed: %w", err)
		}
	}
	// Reads need data to read.
	if w.ReadFraction > 0 {
		for k := 0; k < w.Keys; k++ {
			pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := clients[0].Set(pctx, fmt.Sprintf("bench-%d", k), value, 0)
			cancel()
			if err != nil {
				return Result{}, fmt.Errorf("bench: populate failed: %w", err)
			}
		}
	}

	type workerOut struct {
		lat  []time.Duration
		errs int
	}
	outs := make([]workerOut, len(clients))

	runCtx, stop := context.WithTimeout(ctx, w.Duration)
	defer stop()

	start := time.Now()
	var wg sync.WaitGroup
	for i, cl := range clients {
		wg.Add(1)
		go func(i int, cl *client.Client) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i) + 1))
			out := &outs[i]
			for runCtx.Err() == nil {
				key := fmt.Sprintf("bench-%d", rng.Intn(w.Keys))
				read := rng.Float64() < w.ReadFraction

				// Operations in flight when the time is up still finish.
				octx, cancel := context.WithTimeout(context.Background(), opTimeout)
				t := time.Now()
				var err error
				if read {
					_, _, err = cl.Get(octx, key)
				} else {
					err = cl.Set(octx, key, value, 0)
				}
				d := time.Since(t)
				cancel()

				if err != nil {
					out.errs++
					continue
				}
				out.lat = append(out.lat, d)
			}
		}(i, cl)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	res := Result{Workload: w, Elapsed: elapsed}
	for _, o := range outs {
		all = append(all, o.lat...)
		res.Errors += o.errs
	}
	res.Ops = len(all)
	res.Latency = Summarize(all)
	return res, nil
}
