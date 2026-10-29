package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/store"
	"github.com/sqmi511/raftkv/internal/transport"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	db, err := store.OpenDB(filepath.Join(t.TempDir(), "kv.wal"))
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Serve(ctx, lis, db, 5*time.Second) }()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = db.Close()
	})
	return lis.Addr().String()
}

// cli runs the CLI against addr and returns exit code, stdout, stderr.
func cli(addr string, args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	full := append([]string{"-addr", addr, "-timeout", "5s"}, args...)
	code := run(full, &out, &errb)
	return code, out.String(), errb.String()
}

func TestSetGetDelRoundTrip(t *testing.T) {
	addr := startTestServer(t)

	if code, out, errs := cli(addr, "set", "name", "sami"); code != 0 || strings.TrimSpace(out) != "OK" {
		t.Fatalf("set: code=%d out=%q err=%q", code, out, errs)
	}
	if code, out, errs := cli(addr, "get", "name"); code != 0 || strings.TrimSpace(out) != "sami" {
		t.Fatalf("get: code=%d out=%q err=%q", code, out, errs)
	}
	if code, out, errs := cli(addr, "del", "name"); code != 0 || strings.TrimSpace(out) != "deleted" {
		t.Fatalf("del: code=%d out=%q err=%q", code, out, errs)
	}
	if code, out, errs := cli(addr, "del", "name"); code != 0 || strings.TrimSpace(out) != "not found" {
		t.Fatalf("second del: code=%d out=%q err=%q", code, out, errs)
	}
}

func TestGetMissingExitsOne(t *testing.T) {
	addr := startTestServer(t)

	code, out, errs := cli(addr, "get", "nope")
	if code != 1 || out != "" || !strings.Contains(errs, "not found") {
		t.Fatalf("code=%d out=%q err=%q; want exit 1 with 'not found' on stderr", code, out, errs)
	}
}

func TestSetWithTTL(t *testing.T) {
	addr := startTestServer(t)

	if code, _, errs := cli(addr, "set", "-ttl", "1h", "k", "v"); code != 0 {
		t.Fatalf("set -ttl: code=%d err=%q", code, errs)
	}
	if code, out, _ := cli(addr, "get", "k"); code != 0 || strings.TrimSpace(out) != "v" {
		t.Fatalf("get after set -ttl: code=%d out=%q", code, out)
	}
}

func TestInvalidTTL(t *testing.T) {
	addr := startTestServer(t)

	for _, ttl := range []string{"-ttl=-5s", "-ttl=1ns"} {
		if code, _, errs := cli(addr, "set", ttl, "k", "v"); code != 2 || !strings.Contains(errs, "-ttl") {
			t.Fatalf("%s: code=%d err=%q; want exit 2", ttl, code, errs)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	addr := startTestServer(t)

	cases := map[string][]string{
		"no command":      {},
		"unknown command": {"frobnicate"},
		"get without key": {"get"},
		"get extra arg":   {"get", "a", "b"},
		"set one arg":     {"set", "k"},
		"set three args":  {"set", "a", "b", "c"},
		"del without key": {"del"},
		"del two args":    {"del", "a", "b"},
		"bad set flag":    {"set", "-nope", "k", "v"},
	}
	for name, args := range cases {
		if code, _, _ := cli(addr, args...); code != 2 {
			t.Errorf("%s: exit code = %d, want 2", name, code)
		}
	}
}

func TestUnreachableServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // nothing listens here now

	code, _, errs := cli(addr, "get", "k")
	if code != 1 || !strings.Contains(errs, "cannot reach server") {
		t.Fatalf("code=%d err=%q; want exit 1 with 'cannot reach server'", code, errs)
	}
}

func TestVersionFlag(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-version"}, &out, &errb); code != 0 || !strings.HasPrefix(out.String(), "raftkv ") {
		t.Fatalf("code=%d out=%q", code, out.String())
	}
}

func TestHelpFlag(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-h"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "Usage:") {
		t.Fatalf("code=%d err=%q", code, errb.String())
	}
}
