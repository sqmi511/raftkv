package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sqmi511/raftkv/internal/kv/kvtest"
	"github.com/sqmi511/raftkv/internal/shard"
)

// startGroups runs two single-node clusters and returns the -groups spec.
func startGroups(t *testing.T) string {
	t.Helper()
	a := kvtest.NewCluster(t, 1)
	b := kvtest.NewCluster(t, 1)
	a.WaitLeader()
	b.WaitLeader()
	return "a=" + a.Addrs()[0] + ";b=" + b.Addrs()[0]
}

func cli(spec string, args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"-groups", spec, "-timeout", "20s"}, args...), &out, &errb)
	return code, strings.TrimSpace(out.String()), errb.String()
}

func TestSetGetDelWhere(t *testing.T) {
	spec := startGroups(t)

	if code, out, errs := cli(spec, "set", "name", "sami"); code != 0 || out != "OK" {
		t.Fatalf("set: code=%d out=%q err=%q", code, out, errs)
	}
	if code, out, errs := cli(spec, "get", "name"); code != 0 || out != "sami" {
		t.Fatalf("get: code=%d out=%q err=%q", code, out, errs)
	}

	// "where" must agree with the ring the client uses.
	groups, err := shard.ParseGroups(spec)
	if err != nil {
		t.Fatal(err)
	}
	ring := shard.NewRing(0)
	for _, g := range groups {
		if err := ring.Add(g.Name); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"name", "k1", "k2", "k3", "k4"} {
		want, _ := ring.Locate(key)
		if code, out, _ := cli(spec, "where", key); code != 0 || out != want {
			t.Fatalf("where %s = %q (code %d), want %q", key, out, code, want)
		}
	}

	if code, out, _ := cli(spec, "del", "name"); code != 0 || out != "deleted" {
		t.Fatalf("del: code=%d out=%q", code, out)
	}
	if code, out, errs := cli(spec, "get", "name"); code != 1 || out != "" || !strings.Contains(errs, "not found") {
		t.Fatalf("get after del: code=%d out=%q err=%q; want exit 1 'not found'", code, out, errs)
	}
}

func TestUsageErrors(t *testing.T) {
	cases := map[string][]string{
		"no groups":       {"get", "k"},
		"bad groups":      {"-groups", "oops", "get", "k"},
		"no command":      {"-groups", "a=x:1"},
		"unknown command": {"-groups", "a=x:1", "frob"},
		"get without key": {"-groups", "a=x:1", "get"},
		"set one arg":     {"-groups", "a=x:1", "set", "k"},
		"unknown flag":    {"-nope"},
	}
	for name, args := range cases {
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code != 2 {
			t.Errorf("%s: exit code %d, want 2", name, code)
		}
	}
}

func TestUnreachableGroup(t *testing.T) {
	// Nothing listens on port 1, so the request must fail cleanly and quickly.
	var out, errb bytes.Buffer
	code := run([]string{"-groups", "a=127.0.0.1:1", "-timeout", "1s", "get", "k"}, &out, &errb)
	if code != 1 || !strings.Contains(errb.String(), "error:") {
		t.Fatalf("code=%d err=%q; want exit 1 with an error", code, errb.String())
	}
}

func TestVersionAndHelp(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-version"}, &out, &errb); code != 0 || !strings.HasPrefix(out.String(), "raftkv ") {
		t.Fatalf("-version: code=%d out=%q", code, out.String())
	}
	errb.Reset()
	if code := run([]string{"-h"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "-groups") {
		t.Fatalf("-h: code=%d usage=%q", code, errb.String())
	}
}
