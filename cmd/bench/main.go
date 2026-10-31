// Command raftkv-bench measures raftkv's throughput, latency and failover
// time, and prints a Markdown report for the README.
//
// By default it starts a 3-node cluster inside the process (real gRPC over
// loopback, real WAL with fsync on every write). With -addrs it measures an
// already running cluster instead, without the failover test.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/sqmi511/raftkv/internal/bench"
	"github.com/sqmi511/raftkv/internal/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftkv-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodes := fs.Int("nodes", 3, "size of the in-process cluster")
	duration := fs.Duration("duration", 5*time.Second, "how long each load scenario runs")
	clients := fs.Int("clients", 16, "concurrent clients for the multi-client scenarios")
	valueSize := fs.Int("value-size", 128, "bytes per written value")
	keys := fs.Int("keys", 100, "size of the key space")
	trials := fs.Int("failover-trials", 5, "number of leader crashes to measure (0 = skip)")
	addrs := fs.String("addrs", "", "benchmark an existing cluster (comma-separated addresses) instead of starting one; skips the failover test")
	dataDir := fs.String("data-dir", "", "base directory for in-process node data (default: a temporary directory, removed afterwards)")
	snapshot := fs.Uint64("snapshot-threshold", 0, "applied entries between snapshots (0 = default)")
	electionMin := fs.Duration("election-min", 300*time.Millisecond, "minimum election timeout (in-process cluster)")
	electionMax := fs.Duration("election-max", 600*time.Millisecond, "maximum election timeout (in-process cluster)")
	heartbeat := fs.Duration("heartbeat", 100*time.Millisecond, "heartbeat interval (in-process cluster)")
	out := fs.String("out", "", "also write the report to this file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	switch {
	case *nodes < 1:
		fmt.Fprintln(stderr, "raftkv-bench: -nodes must be at least 1")
		return 2
	case *clients < 1:
		fmt.Fprintln(stderr, "raftkv-bench: -clients must be at least 1")
		return 2
	case *duration <= 0:
		fmt.Fprintln(stderr, "raftkv-bench: -duration must be positive")
		return 2
	case *valueSize < 0 || *keys < 1 || *trials < 0:
		fmt.Fprintln(stderr, "raftkv-bench: -value-size, -keys and -failover-trials must not be negative (and -keys at least 1)")
		return 2
	}
	logf := func(format string, a ...any) { fmt.Fprintf(stderr, format+"\n", a...) }
	fail := func(err error) int {
		fmt.Fprintln(stderr, "raftkv-bench:", err)
		return 1
	}

	report := bench.Report{Env: []string{
		fmt.Sprintf("%s/%s, %d CPUs, %s, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), version.String()),
	}}

	var cluster *bench.Cluster
	var targets []string
	if *addrs != "" {
		for _, a := range strings.Split(*addrs, ",") {
			if a = strings.TrimSpace(a); a != "" {
				targets = append(targets, a)
			}
		}
		if len(targets) == 0 {
			fmt.Fprintln(stderr, "raftkv-bench: -addrs lists no addresses")
			return 2
		}
		report.Env = append(report.Env, fmt.Sprintf("external cluster: %s", strings.Join(targets, ", ")))
	} else {
		dir := *dataDir
		if dir == "" {
			tmp, err := os.MkdirTemp("", "raftkv-bench-")
			if err != nil {
				return fail(err)
			}
			dir = tmp
			defer os.RemoveAll(tmp) // runs after cluster.Close
		}
		c, err := bench.StartCluster(bench.ClusterOptions{
			Size:               *nodes,
			Dir:                dir,
			SnapshotThreshold:  *snapshot,
			ElectionTimeoutMin: *electionMin,
			ElectionTimeoutMax: *electionMax,
			HeartbeatInterval:  *heartbeat,
		})
		if err != nil {
			return fail(err)
		}
		cluster = c
		defer cluster.Close()

		logf("starting a %d-node cluster...", *nodes)
		hctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		_, err = cluster.WaitHealthy(hctx)
		cancel()
		if err != nil {
			return fail(err)
		}
		targets = cluster.Addrs()
		report.Env = append(report.Env,
			fmt.Sprintf("%d-node in-process cluster, real gRPC over loopback, WAL fsynced on every write", *nodes),
			fmt.Sprintf("election timeout %v-%v, heartbeat %v", *electionMin, *electionMax, *heartbeat))
	}
	report.Env = append(report.Env,
		fmt.Sprintf("value size %d B, %d keys, %v per scenario", *valueSize, *keys, *duration))

	mk := func(name string, n int, read float64) bench.Workload {
		return bench.Workload{Name: name, Clients: n, Duration: *duration, ValueSize: *valueSize, ReadFraction: read, Keys: *keys}
	}
	scenarios := []bench.Workload{
		mk("write, 1 client", 1, 0),
		mk(fmt.Sprintf("write, %d clients", *clients), *clients, 0),
		mk(fmt.Sprintf("read (linearizable), %d clients", *clients), *clients, 1),
		mk(fmt.Sprintf("mixed 50/50, %d clients", *clients), *clients, 0.5),
	}
	for _, w := range scenarios {
		logf("running %q for %v...", w.Name, w.Duration)
		res, err := bench.Run(ctx, targets, w)
		if err != nil {
			return fail(err)
		}
		report.Load = append(report.Load, res)
	}

	switch {
	case cluster == nil:
		logf("skipping the failover test: it needs the in-process cluster")
	case *nodes < 3:
		logf("skipping the failover test: it needs at least 3 nodes")
	case *trials > 0:
		logf("measuring failover (%d trials)...", *trials)
		fo, err := bench.MeasureFailover(ctx, cluster, *trials)
		if err != nil {
			return fail(err)
		}
		report.Failover = fo
	}
	if ctx.Err() != nil {
		return fail(errors.New("interrupted"))
	}

	md := report.Markdown()
	fmt.Fprint(stdout, md)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			return fail(err)
		}
	}
	return 0
}
