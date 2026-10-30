package kv_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/kv/kvtest"
	"github.com/sqmi511/raftkv/internal/store"
)

func tctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustSet(t *testing.T, svc *kv.Service, client string, req uint64, key, val string) {
	t.Helper()
	if err := svc.Set(tctx(t, 10*time.Second), client, req, key, []byte(val), 0); err != nil {
		t.Fatalf("Set(%s, %d, %q): %v", client, req, key, err)
	}
}

func mustGet(t *testing.T, svc *kv.Service, key, want string) {
	t.Helper()
	v, ok, err := svc.Get(tctx(t, 10*time.Second), key)
	if err != nil || !ok || string(v) != want {
		t.Fatalf("Get(%q) = %q, %v, %v; want %q", key, v, ok, err, want)
	}
}

func TestWriteThenLinearizableRead(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	leader := c.WaitLeader()
	svc := c.Service(leader)

	if _, ok, err := svc.Get(tctx(t, 5*time.Second), "k"); err != nil || ok {
		t.Fatalf("Get(missing) = %v, %v; want not found", ok, err)
	}
	mustSet(t, svc, "c1", 1, "k", "v1")
	mustGet(t, svc, "k", "v1")
	mustSet(t, svc, "c1", 2, "k", "v2")
	mustGet(t, svc, "k", "v2")

	// The write reached the followers' state machines, not just the leader's.
	for _, f := range c.Followers(leader) {
		fsvc := c.Service(f)
		eventually(t, "follower "+f+" to apply the writes", func() bool {
			v, ok := fsvc.StaleGet("k")
			return ok && string(v) == "v2"
		})
	}
}

func TestFollowersRefuseAndNameTheLeader(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	leader := c.WaitLeader()
	follower := c.Followers(leader)[0]
	svc := c.Service(follower)

	eventually(t, "the follower to learn who leads", func() bool {
		return svc.Node().Status().LeaderID == leader
	})

	var nl *kv.NotLeaderError
	err := svc.Set(tctx(t, 5*time.Second), "c1", 1, "k", []byte("v"), 0)
	if !errors.As(err, &nl) || nl.LeaderID != leader {
		t.Fatalf("Set on a follower = %v, want NotLeaderError naming %s", err, leader)
	}
	_, _, err = svc.Get(tctx(t, 5*time.Second), "k")
	if !errors.As(err, &nl) || nl.LeaderID != leader {
		t.Fatalf("Get on a follower = %v, want NotLeaderError naming %s", err, leader)
	}
}

func TestDuplicateRequestsApplyExactlyOnce(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	svc := c.Service(c.WaitLeader())

	mustSet(t, svc, "c1", 1, "k", "first")
	mustSet(t, svc, "c2", 1, "k", "second") // another client overwrites
	// c1 retries request 1 because its response was lost: it must be
	// acknowledged but NOT overwrite c2's newer value.
	mustSet(t, svc, "c1", 1, "k", "first")
	mustGet(t, svc, "k", "second")

	// A retried Delete returns the original answer, not "already gone".
	mustSet(t, svc, "c3", 1, "d", "x")
	for i := 0; i < 3; i++ {
		deleted, err := svc.Delete(tctx(t, 10*time.Second), "c3", 2, "d")
		if err != nil || !deleted {
			t.Fatalf("Delete attempt %d = %v, %v; want true, nil", i+1, deleted, err)
		}
	}
}

func TestStaleRequestIDIsRejected(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	svc := c.Service(c.WaitLeader())

	mustSet(t, svc, "c1", 5, "k", "newest")
	err := svc.Set(tctx(t, 5*time.Second), "c1", 3, "k", []byte("old"), 0)
	if !errors.Is(err, kv.ErrStaleRequest) {
		t.Fatalf("Set with an old request id = %v, want ErrStaleRequest", err)
	}
	mustGet(t, svc, "k", "newest")
}

func TestInvalidRequests(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	svc := c.Service(c.WaitLeader())
	ctx := tctx(t, 5*time.Second)

	if err := svc.Set(ctx, "", 1, "k", nil, 0); !errors.Is(err, kv.ErrInvalidRequest) {
		t.Fatalf("empty client id = %v, want ErrInvalidRequest", err)
	}
	if err := svc.Set(ctx, "c1", 0, "k", nil, 0); !errors.Is(err, kv.ErrInvalidRequest) {
		t.Fatalf("request id 0 = %v, want ErrInvalidRequest", err)
	}
	if err := svc.Set(ctx, "c1", 1, "", nil, 0); !errors.Is(err, store.ErrEmptyKey) {
		t.Fatalf("empty key = %v, want ErrEmptyKey", err)
	}
	if _, _, err := svc.Get(ctx, ""); !errors.Is(err, store.ErrEmptyKey) {
		t.Fatalf("Get with empty key = %v, want ErrEmptyKey", err)
	}
}

func TestPartitionedLeaderCannotServeStaleReadsOrAckWrites(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	old := c.WaitLeader()
	oldSvc := c.Service(old)
	mustSet(t, oldSvc, "c1", 1, "k", "old")

	// Cut the leader off. It still thinks it leads and still holds "old".
	c.Isolate(old)

	if _, _, err := oldSvc.Get(tctx(t, 400*time.Millisecond), "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get on an isolated leader = %v, want a timeout (it must not answer)", err)
	}
	err := oldSvc.Set(tctx(t, 400*time.Millisecond), "c1", 2, "k", []byte("doomed"), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Set on an isolated leader = %v, want a timeout (it must not acknowledge)", err)
	}

	// Meanwhile the majority elects a new leader and moves on.
	next := c.WaitLeader()
	if next == old {
		t.Fatal("isolated node is still reported as leader")
	}
	mustSet(t, c.Service(next), "c2", 1, "k", "new")
	mustGet(t, c.Service(next), "k", "new")

	// After healing, the old leader converges and the doomed write is gone.
	c.Heal(old)
	eventually(t, "the old leader to converge on the new value", func() bool {
		v, ok := oldSvc.StaleGet("k")
		return ok && string(v) == "new"
	})
	for _, a := range c.Addrs() {
		if v, _ := c.Service(a).StaleGet("k"); string(v) == "doomed" {
			t.Fatalf("%s holds a write that was never committed", a)
		}
	}
}

func TestRetryAfterLeaderChangeGetsOriginalDeleteResult(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	leader := c.WaitLeader()
	svc := c.Service(leader)

	mustSet(t, svc, "c1", 1, "k", "v")
	deleted, err := svc.Delete(tctx(t, 10*time.Second), "c1", 2, "k")
	if err != nil || !deleted {
		t.Fatalf("Delete = %v, %v; want true, nil", deleted, err)
	}

	// The client never saw that answer. The leader dies, a new one is
	// elected, and the client retries the same request there.
	c.Isolate(leader)
	next := c.WaitLeader()
	nextSvc := c.Service(next)

	// A linearizable read forces the new leader to apply everything committed
	// by its predecessor (including the delete and its session record).
	if _, ok, err := nextSvc.Get(tctx(t, 10*time.Second), "k"); err != nil || ok {
		t.Fatalf("Get after failover = %v, %v; want not found", ok, err)
	}
	again, err := nextSvc.Delete(tctx(t, 10*time.Second), "c1", 2, "k")
	if err != nil || !again {
		t.Fatalf("retried Delete on the new leader = %v, %v; want the original answer true", again, err)
	}
}

func TestWriteOnDeposedLeaderFailsInsteadOfHanging(t *testing.T) {
	c := kvtest.NewCluster(t, 3)
	old := c.WaitLeader()
	svc := c.Service(old)

	c.Isolate(old)
	ctx := tctx(t, 30*time.Second)
	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Set(ctx, "c1", 1, "k", []byte("v"), 0)
	}()

	c.WaitLeader() // the other two elect a replacement
	c.Heal(old)    // the old leader learns the new term and steps down

	select {
	case err := <-errCh:
		if !errors.Is(err, kv.ErrLeadershipLost) && !errors.Is(err, kv.ErrOverwritten) {
			t.Fatalf("write on the deposed leader = %v, want ErrLeadershipLost or ErrOverwritten", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the write on the deposed leader never returned")
	}
}
