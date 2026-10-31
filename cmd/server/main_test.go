package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/config"
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

// startServer runs a single-node server in-process. stop shuts it down like a
// SIGTERM would and returns run's result; it is also registered as cleanup.
func startServer(t *testing.T, dataDir string) (grpcAddr, httpAddr string, stop func() error) {
	t.Helper()
	grpcAddr, httpAddr = freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	args := []string{
		"-listen", grpcAddr, "-http-addr", httpAddr, "-data-dir", dataDir,
		"-election-timeout-min", "100ms", "-election-timeout-max", "200ms", "-heartbeat-interval", "30ms",
	}
	go func() { done <- run(ctx, args, io.Discard, io.Discard) }()

	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return grpcAddr, httpAddr, stop
}

var httpClient = &http.Client{Timeout: 2 * time.Second}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func waitReady(t *testing.T, httpAddr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if resp, err := httpClient.Get("http://" + httpAddr + "/readyz"); err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never became ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	cl, err := client.New(client.Options{Addrs: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func TestServerEndToEnd(t *testing.T) {
	grpcAddr, httpAddr, stop := startServer(t, t.TempDir())
	waitReady(t, httpAddr)

	cl := newClient(t, grpcAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cl.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cl.Get(ctx, "k"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get = %q, %v, %v; want v", v, ok, err)
	}

	if code, body := httpGet(t, "http://"+httpAddr+"/healthz"); code != 200 {
		t.Fatalf("/healthz = %d %q", code, body)
	}
	if code, body := httpGet(t, "http://"+httpAddr+"/status"); code != 200 || !strings.Contains(body, `"state": "leader"`) {
		t.Fatalf("/status = %d %q", code, body)
	}
	code, body := httpGet(t, "http://"+httpAddr+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		"raftkv_raft_is_leader 1\n",
		`raftkv_grpc_requests_total{method="/raftkv.v1.KV/Set",code="OK"} 1`,
		`raftkv_grpc_requests_total{method="/raftkv.v1.KV/Get",code="OK"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}

	if err := stop(); err != nil {
		t.Fatalf("run returned %v after shutdown, want nil", err)
	}
	if _, err := httpClient.Get("http://" + httpAddr + "/healthz"); err == nil {
		t.Fatal("the ops endpoint is still up after shutdown")
	}
}

func TestServerRestartKeepsData(t *testing.T) {
	dir := t.TempDir()

	grpcAddr, httpAddr, stop := startServer(t, dir)
	waitReady(t, httpAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := newClient(t, grpcAddr).Set(ctx, "persist", []byte("yes"), 0); err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// A new process on the same data directory (and different ports).
	grpcAddr, httpAddr, _ = startServer(t, dir)
	waitReady(t, httpAddr)
	v, ok, err := newClient(t, grpcAddr).Get(ctx, "persist")
	if err != nil || !ok || string(v) != "yes" {
		t.Fatalf("Get after restart = %q, %v, %v; want yes", v, ok, err)
	}
}

func TestRunRejectsBadInput(t *testing.T) {
	// listing ourselves (the default listen address) as a peer
	err := run(context.Background(), []string{"-peers", "127.0.0.1:7000", "-data-dir", t.TempDir()}, io.Discard, io.Discard)
	if !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("err = %v, want config.ErrInvalid", err)
	}
	if err := run(context.Background(), []string{"-nope"}, io.Discard, io.Discard); err == nil {
		t.Fatal("an unknown flag was accepted")
	}
}

func TestVersionFlag(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "raftkv ") {
		t.Fatalf("output = %q", out.String())
	}
}
