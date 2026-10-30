package client_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/kv/kvtest"
)

func tctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func newClient(t *testing.T, addrs []string, extra ...grpc.DialOption) *client.Client {
	t.Helper()
	cl, err := client.New(client.Options{Addrs: addrs, MaxAttempts: 20, DialOptions: extra})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func mustGet(t *testing.T, cl *client.Client, key, want string) {
	t.Helper()
	v, ok, err := cl.Get(tctx(t, 20*time.Second), key)
	if err != nil || !ok || string(v) != want {
		t.Fatalf("Get(%q) = %q, %v, %v; want %q", key, v, ok, err, want)
	}
}

func TestSetGetDelete(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	c.WaitLeader()
	cl := newClient(t, c.Addrs())
	ctx := tctx(t, 30*time.Second)

	if _, ok, err := cl.Get(ctx, "k"); err != nil || ok {
		t.Fatalf("Get(missing) = %v, %v; want not found", ok, err)
	}
	if err := cl.Set(ctx, "k", []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	mustGet(t, cl, "k", "v1")
	if err := cl.Set(ctx, "k", []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	mustGet(t, cl, "k", "v2")

	if deleted, err := cl.Delete(ctx, "k"); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v; want true", deleted, err)
	}
	if deleted, err := cl.Delete(ctx, "k"); err != nil || deleted {
		t.Fatalf("second Delete = %v, %v; want false", deleted, err)
	}
	if _, ok, _ := cl.Get(ctx, "k"); ok {
		t.Fatal("key still present after Delete")
	}
}

func TestTTL(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	c.WaitLeader()
	cl := newClient(t, c.Addrs())

	if err := cl.Set(tctx(t, 20*time.Second), "temp", []byte("v"), 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, ok, err := cl.Get(tctx(t, 10*time.Second), "temp")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("key did not expire")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	leader := c.WaitLeader()
	follower := c.Followers(leader)[0]

	// The client only knows a follower. It must be told where the leader is.
	cl := newClient(t, []string{follower})
	if err := cl.Set(tctx(t, 30*time.Second), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set via a follower: %v", err)
	}
	mustGet(t, cl, "k", "v")
}

func TestSurvivesLeaderFailure(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	leader := c.WaitLeader()
	cl := newClient(t, c.Addrs())

	for i := 0; i < 10; i++ {
		if err := cl.Set(tctx(t, 30*time.Second), fmt.Sprintf("k%d", i), []byte("before"), 0); err != nil {
			t.Fatal(err)
		}
	}

	c.Stop(leader) // the leader process dies

	// Writes keep working once a new leader is elected, and nothing that was
	// acknowledged before the crash is lost.
	if err := cl.Set(tctx(t, 30*time.Second), "after", []byte("x"), 0); err != nil {
		t.Fatalf("Set after the leader died: %v", err)
	}
	for i := 0; i < 10; i++ {
		mustGet(t, cl, fmt.Sprintf("k%d", i), "before")
	}
	mustGet(t, cl, "after", "x")
}

func TestLostResponseIsRetriedExactlyOnce(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	c.WaitLeader()

	// Simulate a network that executes the first Delete but loses its response.
	var dropped atomic.Bool
	lossy := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && strings.HasSuffix(method, "/Delete") && dropped.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "simulated lost response")
		}
		return err
	})
	cl := newClient(t, c.Addrs(), grpc.WithTransportCredentials(insecure.NewCredentials()), lossy)

	if err := cl.Set(tctx(t, 30*time.Second), "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	deleted, err := cl.Delete(tctx(t, 30*time.Second), "k")
	if err != nil {
		t.Fatal(err)
	}
	if !dropped.Load() {
		t.Fatal("the fault was never injected")
	}
	// Without deduplication the retry would find the key already gone and
	// report false. With it, the original answer is returned.
	if !deleted {
		t.Fatal("retried Delete reported false: the first attempt was lost or applied twice")
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	c.WaitLeader()
	cl := newClient(t, c.Addrs())

	start := time.Now()
	err := cl.Set(tctx(t, 30*time.Second), "", []byte("v"), 0)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Set with an empty key = %v, want InvalidArgument", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("an InvalidArgument error took %v: it must not be retried", time.Since(start))
	}

	if err := cl.Set(tctx(t, 5*time.Second), "k", []byte("v"), -time.Second); !errors.Is(err, client.ErrInvalidTTL) {
		t.Fatalf("negative ttl = %v, want ErrInvalidTTL", err)
	}
	if err := cl.Set(tctx(t, 5*time.Second), "k", []byte("v"), time.Microsecond); !errors.Is(err, client.ErrInvalidTTL) {
		t.Fatalf("sub-millisecond ttl = %v, want ErrInvalidTTL", err)
	}
}

func TestUnreachableClusterGivesUnavailable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // nothing listens here any more

	cl, err := client.New(client.Options{
		Addrs: []string{addr}, MaxAttempts: 3, BackoffMin: 5 * time.Millisecond, BackoffMax: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	_, _, err = cl.Get(tctx(t, 10*time.Second), "k")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Get = %v, want Unavailable", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	c.WaitLeader()

	const (
		workers = 4
		perW    = 10
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl, err := client.New(client.Options{Addrs: c.Addrs(), MaxAttempts: 20})
			if err != nil {
				t.Errorf("New: %v", err)
				return
			}
			defer cl.Close()
			for i := 0; i < perW; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := cl.Set(ctx, fmt.Sprintf("w%d-k%d", w, i), []byte(fmt.Sprint(i)), 0)
				cancel()
				if err != nil {
					t.Errorf("worker %d Set %d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	reader := newClient(t, c.Addrs())
	for w := 0; w < workers; w++ {
		for i := 0; i < perW; i++ {
			mustGet(t, reader, fmt.Sprintf("w%d-k%d", w, i), fmt.Sprint(i))
		}
	}
}

func TestOptionsAndLifecycle(t *testing.T) {
	if _, err := client.New(client.Options{}); !errors.Is(err, client.ErrNoAddrs) {
		t.Fatalf("New without addresses = %v, want ErrNoAddrs", err)
	}
	if _, err := client.New(client.Options{Addrs: []string{""}}); err == nil {
		t.Fatal("an empty address was accepted")
	}
	if _, err := client.New(client.Options{
		Addrs: []string{"a:1"}, BackoffMin: time.Second, BackoffMax: time.Millisecond,
	}); err == nil {
		t.Fatal("BackoffMax below BackoffMin was accepted")
	}

	a, err := client.New(client.Options{Addrs: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.New(client.Options{Addrs: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == "" || a.ID() == b.ID() {
		t.Fatalf("client IDs must be non-empty and unique: %q, %q", a.ID(), b.ID())
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if _, _, err := a.Get(context.Background(), "k"); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	_ = b.Close()
}
