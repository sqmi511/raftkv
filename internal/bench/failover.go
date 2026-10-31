package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/sqmi511/raftkv/client"
)

// FailoverTrial is one leader-crash measurement, both times counted from the
// moment of the crash.
type FailoverTrial struct {
	// Election is when a new leader first existed. It is sampled about once
	// a millisecond, so it is accurate to a few milliseconds (more on
	// Windows, whose timers can be coarser).
	Election time.Duration
	// Write is when a client write first succeeded: the unavailability a
	// real client sees, including the time its retries take to find the
	// new leader.
	Write time.Duration
}

// MeasureFailover crashes the leader `trials` times, restarting it in
// between, and records how long the cluster is unavailable each time.
func MeasureFailover(ctx context.Context, c *Cluster, trials int) ([]FailoverTrial, error) {
	var out []FailoverTrial
	for i := 1; i <= trials; i++ {
		t, err := failoverOnce(ctx, c)
		if err != nil {
			return out, fmt.Errorf("failover trial %d: %w", i, err)
		}
		out = append(out, t)
	}
	return out, nil
}

func failoverOnce(ctx context.Context, c *Cluster) (FailoverTrial, error) {
	hctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	leader, err := c.WaitHealthy(hctx)
	cancel()
	if err != nil {
		return FailoverTrial{}, err
	}
	oldTerm := c.Status(leader).Term

	cl, err := client.New(client.Options{
		Addrs:          c.Addrs(),
		AttemptTimeout: 500 * time.Millisecond,
		BackoffMin:     10 * time.Millisecond,
		BackoffMax:     100 * time.Millisecond,
	})
	if err != nil {
		return FailoverTrial{}, err
	}
	defer cl.Close()

	// The client knows the leader before the crash, like a real one would.
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	err = cl.Set(pctx, "failover-probe", []byte("0"), 0)
	pcancel()
	if err != nil {
		return FailoverTrial{}, fmt.Errorf("probe write before the crash: %w", err)
	}

	t0 := time.Now()
	if err := c.Crash(leader); err != nil {
		return FailoverTrial{}, err
	}

	elected := make(chan time.Duration, 1)
	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go func() {
		for pollCtx.Err() == nil {
			if addr, st, ok := c.Leader(); ok && addr != leader && st.Term > oldTerm {
				elected <- time.Since(t0)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	deadline := time.Now().Add(30 * time.Second)
	for {
		sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
		err := cl.Set(sctx, "failover-probe", []byte("1"), 0)
		scancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return FailoverTrial{}, ctx.Err()
		}
		if time.Now().After(deadline) {
			return FailoverTrial{}, fmt.Errorf("no write succeeded within 30s of the crash: %w", err)
		}
	}
	write := time.Since(t0)

	var election time.Duration
	select {
	case election = <-elected:
	case <-time.After(2 * time.Second):
		election = write
	}
	if election > write { // sampling jitter: a write cannot succeed before a leader exists
		election = write
	}

	if err := c.Restart(leader); err != nil {
		return FailoverTrial{}, err
	}
	return FailoverTrial{Election: election, Write: write}, nil
}
