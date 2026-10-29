// Command raftkv-cli is the command-line client for raftkv.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/internal/version"
)

const usage = `Usage: raftkv-cli [-addr host:port] [-timeout 5s] <command> [args]

Commands:
  get <key>                      print the value of key
  set [-ttl 30s] <key> <value>   store value under key (flags go before arguments)
  del <key>                      delete key

Exit codes: 0 success, 1 failure or key not found, 2 usage error.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type action func(ctx context.Context, c kvpb.KVClient) (exitCode int, err error)

func run(args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("raftkv-cli", flag.ContinueOnError)
	global.SetOutput(stderr)
	global.Usage = func() { fmt.Fprint(stderr, usage) }
	addr := global.String("addr", "127.0.0.1:7000", "server address")
	timeout := global.Duration("timeout", 5*time.Second, "per-request timeout")
	showVersion := global.Bool("version", false, "print version and exit")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, version.String())
		return 0
	}

	rest := global.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, cmdArgs := rest[0], rest[1:]

	var do action
	switch cmd {
	case "get":
		if len(cmdArgs) != 1 {
			return usageError(stderr, "usage: raftkv-cli get <key>")
		}
		key := cmdArgs[0]
		do = func(ctx context.Context, c kvpb.KVClient) (int, error) {
			resp, err := c.Get(ctx, &kvpb.GetRequest{Key: key})
			if err != nil {
				return 0, err
			}
			if !resp.GetFound() {
				fmt.Fprintln(stderr, "not found")
				return 1, nil
			}
			fmt.Fprintln(stdout, string(resp.GetValue()))
			return 0, nil
		}

	case "set":
		fs := flag.NewFlagSet("set", flag.ContinueOnError)
		fs.SetOutput(stderr)
		ttl := fs.Duration("ttl", 0, "expire the key after this duration, e.g. 30s or 5m (0 = never)")
		if err := fs.Parse(cmdArgs); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return 0
			}
			return 2
		}
		if fs.NArg() != 2 {
			return usageError(stderr, "usage: raftkv-cli set [-ttl 30s] <key> <value>")
		}
		if *ttl < 0 || (*ttl > 0 && *ttl < time.Millisecond) {
			return usageError(stderr, "-ttl must be 0 or at least 1ms")
		}
		key, value, ttlMillis := fs.Arg(0), fs.Arg(1), ttl.Milliseconds()
		do = func(ctx context.Context, c kvpb.KVClient) (int, error) {
			if _, err := c.Set(ctx, &kvpb.SetRequest{Key: key, Value: []byte(value), TtlMillis: ttlMillis}); err != nil {
				return 0, err
			}
			fmt.Fprintln(stdout, "OK")
			return 0, nil
		}

	case "del":
		if len(cmdArgs) != 1 {
			return usageError(stderr, "usage: raftkv-cli del <key>")
		}
		key := cmdArgs[0]
		do = func(ctx context.Context, c kvpb.KVClient) (int, error) {
			resp, err := c.Delete(ctx, &kvpb.DeleteRequest{Key: key})
			if err != nil {
				return 0, err
			}
			if resp.GetDeleted() {
				fmt.Fprintln(stdout, "deleted")
			} else {
				fmt.Fprintln(stdout, "not found")
			}
			return 0, nil
		}

	default:
		return usageError(stderr, fmt.Sprintf("unknown command %q\n\n%s", cmd, usage))
	}

	// NOTE: plaintext transport; TLS/mTLS is a planned hardening step.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	code, err := do(ctx, kvpb.NewKVClient(conn))
	if err != nil {
		fmt.Fprintln(stderr, "error:", describe(err))
		return 1
	}
	return code
}

func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintln(stderr, msg)
	return 2
}

func describe(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "cannot reach server"
	case codes.DeadlineExceeded:
		return "request timed out"
	default:
		return status.Convert(err).Message()
	}
}
