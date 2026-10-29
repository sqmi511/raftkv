package transport

import (
	"context"
	"math"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/internal/store"
)

func openTestDB(t *testing.T, path string) *store.DB {
	t.Helper()
	db, err := store.OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// startServer serves db on a random local port. stop() triggers a graceful
// shutdown and waits for Serve to return; it is also registered as cleanup.
func startServer(t *testing.T, db *store.DB) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, lis, db, 5*time.Second) }()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Serve returned %v, want nil", err)
			}
		})
	}
	t.Cleanup(stop)
	return lis.Addr().String(), stop
}

func dial(t *testing.T, addr string) kvpb.KVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return kvpb.NewKVClient(conn)
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %v (err = %v), want %v", got, err, want)
	}
}

func TestSetGetDelete(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, _ := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	if r, err := c.Get(ctx, &kvpb.GetRequest{Key: "k"}); err != nil || r.Found {
		t.Fatalf("Get(missing) = %v, %v; want not found", r, err)
	}

	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v1")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	r, err := c.Get(ctx, &kvpb.GetRequest{Key: "k"})
	if err != nil || !r.Found || string(r.Value) != "v1" {
		t.Fatalf("Get = %v, %v; want v1", r, err)
	}

	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}
	r, _ = c.Get(ctx, &kvpb.GetRequest{Key: "k"})
	if string(r.Value) != "v2" {
		t.Fatalf("after overwrite got %q, want v2", r.Value)
	}

	d, err := c.Delete(ctx, &kvpb.DeleteRequest{Key: "k"})
	if err != nil || !d.Deleted {
		t.Fatalf("Delete = %v, %v; want deleted", d, err)
	}
	d, err = c.Delete(ctx, &kvpb.DeleteRequest{Key: "k"})
	if err != nil || d.Deleted {
		t.Fatalf("second Delete = %v, %v; want not deleted", d, err)
	}
	if r, _ := c.Get(ctx, &kvpb.GetRequest{Key: "k"}); r.Found {
		t.Fatal("key still present after Delete")
	}
}

func TestEmptyAndBinaryValues(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, _ := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "empty"}); err != nil {
		t.Fatal(err)
	}
	r, err := c.Get(ctx, &kvpb.GetRequest{Key: "empty"})
	if err != nil || !r.Found || len(r.Value) != 0 {
		t.Fatalf("Get(empty) = %v, %v; want found with empty value", r, err)
	}

	bin := []byte{0, 1, 2, 0xFF, 0}
	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "bin", Value: bin}); err != nil {
		t.Fatal(err)
	}
	r, _ = c.Get(ctx, &kvpb.GetRequest{Key: "bin"})
	if string(r.Value) != string(bin) {
		t.Fatalf("binary value mangled: %v", r.Value)
	}
}

func TestTTL(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, _ := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v"), TtlMillis: 500}); err != nil {
		t.Fatal(err)
	}
	if r, _ := c.Get(ctx, &kvpb.GetRequest{Key: "k"}); !r.Found {
		t.Fatal("key should exist right after Set")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := c.Get(ctx, &kvpb.GetRequest{Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if !r.Found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("key did not expire")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestInvalidArguments(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, _ := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	_, err := c.Get(ctx, &kvpb.GetRequest{})
	wantCode(t, err, codes.InvalidArgument)

	_, err = c.Set(ctx, &kvpb.SetRequest{Value: []byte("v")})
	wantCode(t, err, codes.InvalidArgument)

	_, err = c.Delete(ctx, &kvpb.DeleteRequest{})
	wantCode(t, err, codes.InvalidArgument)

	_, err = c.Set(ctx, &kvpb.SetRequest{Key: "k", TtlMillis: -1})
	wantCode(t, err, codes.InvalidArgument)

	_, err = c.Set(ctx, &kvpb.SetRequest{Key: "k", TtlMillis: math.MaxInt64})
	wantCode(t, err, codes.InvalidArgument)
}

func TestClosedDBMapsToUnavailable(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, _ := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	// The key must exist before Close: deleting a missing key is a no-op
	// that never touches the (closed) log.
	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := c.Set(ctx, &kvpb.SetRequest{Key: "k2", Value: []byte("v")})
	wantCode(t, err, codes.Unavailable)
	_, err = c.Delete(ctx, &kvpb.DeleteRequest{Key: "k"})
	wantCode(t, err, codes.Unavailable)
}
func TestDataSurvivesServerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.wal")
	ctx := testCtx(t)

	db1 := openTestDB(t, path)
	addr1, stop1 := startServer(t, db1)
	c1 := dial(t, addr1)
	if _, err := c1.Set(ctx, &kvpb.SetRequest{Key: "durable", Value: []byte("yes")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Set(ctx, &kvpb.SetRequest{Key: "removed", Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Delete(ctx, &kvpb.DeleteRequest{Key: "removed"}); err != nil {
		t.Fatal(err)
	}
	stop1()
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openTestDB(t, path)
	addr2, _ := startServer(t, db2)
	c2 := dial(t, addr2)

	r, err := c2.Get(ctx, &kvpb.GetRequest{Key: "durable"})
	if err != nil || !r.Found || string(r.Value) != "yes" {
		t.Fatalf("Get(durable) = %v, %v; want yes", r, err)
	}
	if r, _ := c2.Get(ctx, &kvpb.GetRequest{Key: "removed"}); r.Found {
		t.Fatal("deleted key came back after restart")
	}
}

func TestGracefulShutdown(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	addr, stop := startServer(t, db)
	c := dial(t, addr)
	ctx := testCtx(t)

	if _, err := c.Set(ctx, &kvpb.SetRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}

	stop() // cancels the context and requires Serve to return nil

	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("listener still accepting connections after shutdown")
	}
	_, err := c.Get(ctx, &kvpb.GetRequest{Key: "k"})
	wantCode(t, err, codes.Unavailable)
}

func TestServeReturnsListenerError(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), "kv.wal"))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = lis.Close() // broken listener

	if err := Serve(context.Background(), lis, db, time.Second); err == nil {
		t.Fatal("Serve on a closed listener should return an error")
	}
}
