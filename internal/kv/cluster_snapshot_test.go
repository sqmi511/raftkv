package kv_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/kv/kvtest"
)

func TestLaggingNodeCatchesUpThroughSnapshot(t *testing.T) {
	c := kvtest.NewClusterWith(t, 3, kvtest.Options{SnapshotThreshold: 10})
	leader := c.WaitLeader()
	svc := c.Service(leader)
	lagging := c.Followers(leader)[0]

	mustSet(t, svc, "c1", 1, "seed", "x")
	c.Isolate(lagging)

	const n = 60
	for i := 0; i < n; i++ {
		mustSet(t, svc, "c1", uint64(i+2), fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	eventually(t, "the leader to compact its log", func() bool {
		return svc.Node().Status().SnapshotIndex > 0
	})

	// The log the lagging node needs is gone, so only a snapshot can help.
	c.Heal(lagging)
	lsvc := c.Service(lagging)
	eventually(t, "the lagging node to catch up", func() bool {
		v, ok := lsvc.StaleGet(fmt.Sprintf("k%d", n-1))
		return ok && string(v) == fmt.Sprint(n-1)
	})
	if lsvc.Node().Status().SnapshotIndex == 0 {
		t.Fatal("the lagging node caught up without installing a snapshot")
	}

	// Kill whichever node leads now. The survivors (one restored from a
	// snapshot) must still serve every write, and accept new ones.
	cl, err := client.New(client.Options{Addrs: c.Addrs(), MaxAttempts: 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	c.Stop(c.WaitLeader())
	ctx := tctx(t, 60*time.Second)
	for i := 0; i < n; i++ {
		v, ok, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !ok || string(v) != fmt.Sprint(i) {
			t.Fatalf("Get(k%d) = %q, %v, %v; want %d", i, v, ok, err, i)
		}
	}
	if err := cl.Set(ctx, "after", []byte("y"), 0); err != nil {
		t.Fatalf("Set after failover: %v", err)
	}
	if v, ok, err := cl.Get(ctx, "after"); err != nil || !ok || string(v) != "y" {
		t.Fatalf("Get(after) = %q, %v, %v; want y", v, ok, err)
	}
}
