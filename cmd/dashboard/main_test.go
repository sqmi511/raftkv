package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestServesTheClusterAndShutsDownCleanly(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"id":"a","state":"leader","term":1,"leader":"a","last_index":1,"commit_index":1,"applied_index":1,"snapshot_index":0,"ready":true}`)
	}))
	defer node.Close()

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	var out, errb bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-listen", addr, "-nodes", node.URL, "-interval", "20ms"}, &out, &errb)
	}()

	var state struct {
		Summary struct{ Leader string } `json:"summary"`
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/api/state")
		if err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&state)
			_ = resp.Body.Close()
			if state.Summary.Leader == "a" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the dashboard never showed the leader (stderr: %s)", errb.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// An open event stream must not block shutdown.
	resp, err := http.Get("http://" + addr + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code %d, stderr: %s", code, errb.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
	if !strings.Contains(out.String(), "dashboard: http://"+addr) {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestUsageErrors(t *testing.T) {
	cases := map[string][]string{
		"unknown flag":     {"-nope"},
		"bad node list":    {"-nodes", "ftp://x:1"},
		"empty node list":  {"-nodes", " , "},
		"zero interval":    {"-interval", "0s"},
		"extra argument":   {"extra"},
		"unusable address": {"-listen", "not an address"},
	}
	for name, args := range cases {
		var out, errb bytes.Buffer
		code := run(context.Background(), args, &out, &errb)
		if code == 0 {
			t.Errorf("%s: exit code 0, want a failure", name)
		}
	}
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"-nodes", "ftp://x:1"}, &out, &errb); code != 2 {
		t.Errorf("bad -nodes: exit code %d, want 2", code)
	}
}

func TestVersionAndHelp(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(context.Background(), []string{"-version"}, &out, &errb); code != 0 || !strings.HasPrefix(out.String(), "raftkv ") {
		t.Fatalf("-version: code %d, out %q", code, out.String())
	}
	out.Reset()
	errb.Reset()
	if code := run(context.Background(), []string{"-h"}, &out, &errb); code != 0 || !strings.Contains(errb.String(), "-nodes") {
		t.Fatalf("-h: code %d, usage %q", code, errb.String())
	}
}
