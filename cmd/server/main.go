// Command raftkv-server runs one raftkv cluster node.
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
	"path/filepath"
	"syscall"
	"time"

	"github.com/sqmi511/raftkv/internal/config"
	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/metrics"
	"github.com/sqmi511/raftkv/internal/ops"
	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/transport"
	"github.com/sqmi511/raftkv/internal/version"
)

func main() { os.Exit(realMain()) }

func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	default:
		fmt.Fprintln(os.Stderr, "raftkv-server:", err)
		return 1
	}
}

// run starts a node and blocks until ctx is cancelled (or startup fails).
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg, err := config.Parse(args, stderr)
	if err != nil {
		return err
	}
	if cfg.ShowVersion {
		fmt.Fprintln(stdout, version.String())
		return nil
	}

	logger := cfg.Logger(stderr)
	nodeLog := logger.With("node", cfg.Advertise)

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	storage, err := raft.OpenWALStorage(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return fmt.Errorf("open raft storage: %w", err)
	}
	defer storage.Close() // runs last

	rc := transport.NewRaftClient()
	defer rc.Close()

	svc, err := kv.New(kv.Config{
		SnapshotThreshold: cfg.SnapshotThreshold,
		Raft: raft.Config{
			ID:                 cfg.Advertise,
			Peers:              cfg.Peers,
			Transport:          rc,
			Storage:            storage,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			Logger:             logger,
		},
	})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	defer svc.Stop() // runs first, before the transport and storage close

	reg := metrics.NewRegistry()
	ops.RegisterNodeMetrics(reg, svc.Node().Status)
	serverMetrics := transport.NewServerMetrics(reg, logger)

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		hl, err := net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			_ = lis.Close()
			return fmt.Errorf("listen (http): %w", err)
		}
		httpSrv = &http.Server{
			Handler:           ops.Handler(svc.Node().Status, reg),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := httpSrv.Serve(hl); err != nil && !errors.Is(err, http.ErrServerClosed) {
				nodeLog.Error("http server failed", "err", err)
			}
		}()
		nodeLog.Info("ops endpoint", "addr", hl.Addr().String())
	}

	svc.Start()
	nodeLog.Info("serving",
		"listen", lis.Addr().String(), "advertise", cfg.Advertise, "peers", cfg.Peers,
		"data_dir", cfg.DataDir, "version", version.Version)

	serveErr := transport.ServeCluster(ctx, lis, svc, cfg.ShutdownGrace,
		transport.ClusterOptions{Metrics: serverMetrics})

	if httpSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = httpSrv.Shutdown(sctx)
		cancel()
	}
	if serveErr != nil {
		return serveErr
	}
	nodeLog.Info("shut down cleanly")
	return nil
}
