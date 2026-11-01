// Command raftkv-shardcli is a command-line client for several raftkv
// clusters ("groups") at once: it hashes each key onto a consistent-hash ring
// and talks to the group that owns it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/shard"
	"github.com/sqmi511/raftkv/internal/version"
)

const usage = `Usage: raftkv-shardcli -groups "name=addr,addr;name=addr" [-timeout 5s] <command> [args]

Commands:
  get <key>           print the value of key
  set <key> <value>   store value under key
  del <key>           delete key
  where <key>         print which group owns key (no network access)

Every client of the same data must use the same group NAMES. Changing the
list of groups changes who owns some keys but does not move any data.

Exit codes: 0 success, 1 failure or key not found, 2 usage error.
`

var commandUsage = map[string]string{
	"get":   "usage: raftkv-shardcli get <key>",
	"set":   "usage: raftkv-shardcli set <key> <value>",
	"del":   "usage: raftkv-shardcli del <key>",
	"where": "usage: raftkv-shardcli where <key>",
}

var commandArgs = map[string]int{"get": 1, "set": 2, "del": 1, "where": 1}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftkv-shardcli", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	spec := fs.String("groups", "", `groups as "name=addr,addr;name=addr"`)
	timeout := fs.Duration("timeout", 5*time.Second, "overall time limit for the request, retries included")
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

	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, cmdArgs := rest[0], rest[1:]
	want, known := commandArgs[cmd]
	if !known {
		return usageError(stderr, fmt.Sprintf("unknown command %q\n\n%s", cmd, usage))
	}
	if len(cmdArgs) != want {
		return usageError(stderr, commandUsage[cmd])
	}

	groups, err := shard.ParseGroups(*spec)
	if err != nil {
		return usageError(stderr, "raftkv-shardcli: -groups: "+err.Error())
	}
	// NOTE: plaintext transport; TLS/mTLS is a planned hardening step.
	sc, err := shard.New(groups, client.Options{})
	if err != nil {
		return usageError(stderr, "raftkv-shardcli: "+err.Error())
	}
	defer sc.Close()

	key := cmdArgs[0]
	if cmd == "where" {
		fmt.Fprintln(stdout, sc.Locate(key))
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd {
	case "get":
		value, found, err := sc.Get(ctx, key)
		if err != nil {
			return failure(stderr, err)
		}
		if !found {
			fmt.Fprintln(stderr, "not found")
			return 1
		}
		fmt.Fprintln(stdout, string(value))
	case "set":
		if err := sc.Set(ctx, key, []byte(cmdArgs[1]), 0); err != nil {
			return failure(stderr, err)
		}
		fmt.Fprintln(stdout, "OK")
	case "del":
		deleted, err := sc.Delete(ctx, key)
		if err != nil {
			return failure(stderr, err)
		}
		if deleted {
			fmt.Fprintln(stdout, "deleted")
		} else {
			fmt.Fprintln(stdout, "not found")
		}
	}
	return 0
}

func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintln(stderr, msg)
	return 2
}

func failure(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, "error:", describe(err))
	return 1
}

func describe(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "request timed out"
	}
	switch status.Code(err) {
	case codes.Unavailable:
		return "cannot reach the group that owns this key (or it has no leader)"
	case codes.DeadlineExceeded:
		return "request timed out"
	default:
		return status.Convert(err).Message()
	}
}
