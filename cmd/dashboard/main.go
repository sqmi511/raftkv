// Command raftkv-dashboard serves a live web page that shows the state of a
// raftkv cluster: which node leads, each node's term and log positions, which
// nodes are down, and a log of changes. It only observes (it polls each
// node's /status endpoint) and never talks Raft.
//
// It has no authentication, so it listens on localhost by default.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sqmi511/raftkv/internal/dashboard"
	"github.com/sqmi511/raftkv/internal/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftkv-dashboard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "127.0.0.1:9000", "address to serve the dashboard on")
	nodes := fs.String("nodes", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003",
		"comma-separated ops endpoints (the nodes' -http-addr) to watch")
	interval := fs.Duration("interval", 500*time.Millisecond, "how often to poll the nodes")
	timeout := fs.Duration("timeout", time.Second, "per-request timeout when polling a node")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.String())
		return 0
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "raftkv-dashboard: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *interval <= 0 || *timeout <= 0 {
		fmt.Fprintln(stderr, "raftkv-dashboard: -interval and -timeout must be positive")
		return 2
	}

	targets, err := dashboard.ParseTargets(*nodes)
	if err != nil {
		fmt.Fprintln(stderr, "raftkv-dashboard:", err)
		return 2
	}
	poller, err := dashboard.NewPoller(dashboard.Options{Targets: targets, Interval: *interval, Timeout: *timeout})
	if err != nil {
		fmt.Fprintln(stderr, "raftkv-dashboard:", err)
		return 2
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(stderr, "raftkv-dashboard:", err)
		return 1
	}
	if host, _, err := net.SplitHostPort(lis.Addr().String()); err != nil || !isLoopback(host) {
		fmt.Fprintln(stderr, "raftkv-dashboard: warning: listening on a non-loopback address; the dashboard has no authentication")
	}

	srv := dashboard.NewServer(poller)
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}

	pctx, stopPolling := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		poller.Run(pctx)
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(lis) }()

	fmt.Fprintf(stdout, "dashboard: http://%s (watching %d node(s))\n", lis.Addr(), len(targets))

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		stopPolling()
		wg.Wait()
		fmt.Fprintln(stderr, "raftkv-dashboard:", err)
		return 1
	}

	srv.Close() // end the live streams so Shutdown does not wait for them
	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = httpSrv.Shutdown(sctx)
	cancel()
	stopPolling()
	wg.Wait()
	return 0
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
