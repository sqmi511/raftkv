package bench

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/client"
)

func TestSummarize(t *testing.T) {
	var samples []time.Duration
	for i := 100; i >= 1; i-- { // deliberately unsorted
		samples = append(samples, time.Duration(i)*time.Millisecond)
	}
	s := Summarize(samples)

	ms := time.Millisecond
	if s.Count != 100 || s.Min != 1*ms || s.Max != 100*ms || s.P50 != 50*ms ||
		s.P95 != 95*ms || s.P99 != 99*ms || s.Mean != 50500*time.Microsecond {
		t.Fatalf("Summarize = %+v", s)
	}
	if samples[0] != 100*ms {
		t.Fatal("Summarize modified its input")
	}

	if got := Summarize(nil); got != (Summary{}) {
		t.Fatalf("Summarize(nil) = %+v, want the zero Summary", got)
	}
	one := Summarize([]time.Duration{5 * ms})
	if one.P50 != 5*ms || one.P99 != 5*ms || one.Max != 5*ms || one.Mean != 5*ms {
		t.Fatalf("one sample: %+v", one)
	}
	two := Summarize([]time.Duration{1 * ms, 9 * ms})
	if two.P50 != 1*ms || two.P99 != 9*ms {
		t.Fatalf("two samples: %+v, want p50 = 1ms and p99 = 9ms", two)
	}
}

func TestWorkloadValidation(t *testing.T) {
	good := Workload{Name: "w", Clients: 1, Duration: time.Second, Keys: 1}
	bad := map[string]func(*Workload){
		"no clients":         func(w *Workload) { w.Clients = 0 },
		"no duration":        func(w *Workload) { w.Duration = 0 },
		"negative size":      func(w *Workload) { w.ValueSize = -1 },
		"no keys":            func(w *Workload) { w.Keys = 0 },
		"read fraction high": func(w *Workload) { w.ReadFraction = 1.5 },
		"read fraction low":  func(w *Workload) { w.ReadFraction = -0.1 },
	}
	for name, mutate := range bad {
		w := good
		mutate(&w)
		if _, err := Run(context.Background(), nil, w); !errors.Is(err, ErrInvalidWorkload) {
			t.Errorf("%s: err = %v, want ErrInvalidWorkload", name, err)
		}
	}
}

func TestClusterOptionsValidation(t *testing.T) {
	if _, err := StartCluster(ClusterOptions{}); err == nil {
		t.Fatal("a cluster without a Dir was accepted")
	}
	if _, err := StartCluster(ClusterOptions{Size: -1, Dir: t.TempDir()}); err == nil {
		t.Fatal("a negative cluster size was accepted")
	}
}

func fastCluster(t *testing.T, size int) *Cluster {
	t.Helper()
	dir := t.TempDir() // registered first, so it is removed after Close
	c, err := StartCluster(ClusterOptions{
		Size:               size,
		Dir:                dir,
		SnapshotThreshold:  20, // small, so snapshots happen under load
		ElectionTimeoutMin: 200 * time.Millisecond,
		ElectionTimeoutMax: 400 * time.Millisecond,
		HeartbeatInterval:  40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func waitHealthy(t *testing.T, c *Cluster) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader, err := c.WaitHealthy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return leader
}

func TestLoadAndFailoverOnRealCluster(t *testing.T) {
	c := fastCluster(t, 3)
	waitHealthy(t, c)
	ctx := context.Background()

	var report Report
	for _, w := range []Workload{
		{Name: "write", Clients: 2, Duration: 500 * time.Millisecond, ValueSize: 32, Keys: 10},
		{Name: "read", Clients: 2, Duration: 500 * time.Millisecond, ValueSize: 32, Keys: 10, ReadFraction: 1},
		{Name: "mixed", Clients: 2, Duration: 500 * time.Millisecond, ValueSize: 32, Keys: 10, ReadFraction: 0.5},
	} {
		res, err := Run(ctx, c.Addrs(), w)
		if err != nil {
			t.Fatalf("%s: %v", w.Name, err)
		}
		if res.Ops == 0 || res.Errors != 0 || res.Latency.Count != res.Ops || res.OpsPerSec() <= 0 {
			t.Fatalf("%s: unexpected result %+v", w.Name, res)
		}
		if res.Latency.P99 < res.Latency.P50 || res.Latency.Max < res.Latency.P99 {
			t.Fatalf("%s: percentiles out of order: %+v", w.Name, res.Latency)
		}
		report.Load = append(report.Load, res)
	}

	trials, err := MeasureFailover(ctx, c, 2)
	if err != nil {
		t.Fatalf("MeasureFailover: %v\n%s", err, c.dump())
	}
	if len(trials) != 2 {
		t.Fatalf("got %d trials, want 2", len(trials))
	}
	for i, tr := range trials {
		if tr.Write <= 0 || tr.Election <= 0 || tr.Election > tr.Write || tr.Write > 20*time.Second {
			t.Fatalf("trial %d is implausible: %+v", i+1, tr)
		}
	}
	report.Failover = trials

	// The cluster is whole again, and no write was lost on the way.
	waitHealthy(t, c)
	md := report.Markdown()
	for _, want := range []string{"| Workload |", "| write |", "| read |", "| mixed |", "| **median** |", "First successful write"} {
		if !strings.Contains(md, want) {
			t.Errorf("report is missing %q:\n%s", want, md)
		}
	}
}

func TestCrashAndRestartKeepData(t *testing.T) {
	c := fastCluster(t, 1)
	addr := waitHealthy(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl, err := client.New(client.Options{Addrs: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	_ = cl.Close()

	if err := c.Restart(addr); err == nil {
		t.Fatal("restarting a running node should fail")
	}
	if err := c.Crash(addr); err != nil {
		t.Fatal(err)
	}
	if err := c.Crash(addr); err == nil {
		t.Fatal("crashing a crashed node should fail")
	}
	if err := c.Crash("127.0.0.1:1"); err == nil {
		t.Fatal("crashing an unknown node should fail")
	}
	if err := c.Restart(addr); err != nil {
		t.Fatal(err)
	}
	waitHealthy(t, c)

	cl, err = client.New(client.Options{Addrs: []string{addr}, MaxAttempts: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if v, ok, err := cl.Get(ctx, "k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get after restart = %q, %v, %v; want v", v, ok, err)
	}
}

func TestMarkdown(t *testing.T) {
	r := Report{
		Env: []string{"3 nodes"},
		Load: []Result{{
			Workload: Workload{Name: "w", Clients: 2},
			Ops:      1000,
			Elapsed:  2 * time.Second,
			Latency:  Summary{P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 3 * time.Millisecond, Max: 4 * time.Millisecond},
		}},
		Failover: []FailoverTrial{
			{Election: 400 * time.Millisecond, Write: 450 * time.Millisecond},
			{Election: 600 * time.Millisecond, Write: 700 * time.Millisecond},
		},
	}
	md := r.Markdown()
	for _, want := range []string{
		"- 3 nodes",
		"| w | 2 | 500 | 1.00 | 2.00 | 3.00 | 4.00 | 0 |",
		"| 1 | 400.0 | 450.0 |",
		"| **median** | 400.0 | 450.0 |",
		"| **max** | 600.0 | 700.0 |",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report is missing %q:\n%s", want, md)
		}
	}
	if (Result{}).OpsPerSec() != 0 {
		t.Fatal("OpsPerSec of an empty result should be 0")
	}
}
