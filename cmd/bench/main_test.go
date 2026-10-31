package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunEndToEnd(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "report.md")
	var out, errb bytes.Buffer
	code := run(context.Background(), []string{
		"-duration", "300ms", "-clients", "2", "-keys", "5", "-failover-trials", "1",
		"-election-min", "200ms", "-election-max", "400ms", "-heartbeat", "40ms",
		"-out", outFile,
	}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit code %d\nstdout:\n%s\nstderr:\n%s", code, out.String(), errb.String())
	}

	for _, want := range []string{
		"| Workload |", "write, 1 client", "read (linearizable), 2 clients", "mixed 50/50, 2 clients",
		"First successful write", "**median**",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report is missing %q:\n%s", want, out.String())
		}
	}
	saved, err := os.ReadFile(outFile)
	if err != nil || string(saved) != out.String() {
		t.Fatalf("-out file differs from stdout (err %v)", err)
	}
}

func TestUsageErrors(t *testing.T) {
	cases := map[string][]string{
		"unknown flag":  {"-nope"},
		"zero nodes":    {"-nodes", "0"},
		"zero clients":  {"-clients", "0"},
		"zero duration": {"-duration", "0s"},
		"no keys":       {"-keys", "0"},
		"empty addrs":   {"-addrs", " , "},
	}
	for name, args := range cases {
		var out, errb bytes.Buffer
		if code := run(context.Background(), args, &out, &errb); code != 2 {
			t.Errorf("%s: exit code %d, want 2", name, code)
		}
	}
}

func TestHelp(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"-h"}, &out, &errb); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
	if !strings.Contains(errb.String(), "-failover-trials") {
		t.Fatalf("usage text is missing flags:\n%s", errb.String())
	}
}
