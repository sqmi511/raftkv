// Command raftkv-server runs a raftkv node.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sqmi511/raftkv/internal/store"
	"github.com/sqmi511/raftkv/internal/transport"
	"github.com/sqmi511/raftkv/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv-server:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:7000", "address to listen on")
	dataDir := flag.String("data-dir", "data", "directory for the write-ahead log")
	grace := flag.Duration("shutdown-grace", 10*time.Second, "max time to wait for in-flight requests on shutdown")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return nil
	}

	if err := os.MkdirAll(*dataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	db, err := store.OpenDB(filepath.Join(*dataDir, "kv.wal"))
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	rec := db.Recovery()
	slog.Info("database opened",
		"records_replayed", rec.Records,
		"bytes_truncated", rec.TruncatedBytes)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("serving", "addr", lis.Addr().String(), "version", version.Version)
	if err := transport.Serve(ctx, lis, db, *grace); err != nil {
		return err
	}
	slog.Info("shut down cleanly")
	return nil
}
