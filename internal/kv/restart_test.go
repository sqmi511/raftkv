package kv_test

import (
	"fmt"
	"testing"

	"github.com/sqmi511/raftkv/internal/kv/kvtest"
)

func TestRestartedNodeRecoversAndCatchesUp(t *testing.T) {
	c := kvtest.NewClusterWith(t, 3, kvtest.Options{SnapshotThreshold: 10})
	leader := c.WaitLeader()
	svc := c.Service(leader)
	victim := c.Followers(leader)[0]

	for i := 0; i < 5; i++ {
		mustSet(t, svc, "c1", uint64(i+1), fmt.Sprintf("k%d", i), "v")
	}
	c.Stop(victim)
	if !c.IsDown(victim) {
		t.Fatal("a stopped node should be reported as down")
	}
	// Enough writes that the others compact their logs past the victim's.
	for i := 5; i < 40; i++ {
		mustSet(t, svc, "c1", uint64(i+1), fmt.Sprintf("k%d", i), "v")
	}

	if err := c.Restart(victim); err != nil {
		t.Fatal(err)
	}
	vsvc := c.Service(victim)
	eventually(t, "the restarted node to catch up", func() bool {
		_, ok := vsvc.StaleGet("k39")
		return ok
	})
	for i := 0; i < 40; i++ {
		if _, ok := vsvc.StaleGet(fmt.Sprintf("k%d", i)); !ok {
			t.Fatalf("restarted node is missing k%d", i)
		}
	}
	if c.IsDown(victim) {
		t.Fatal("a restarted node should not be reported as down")
	}

	if err := c.Restart(victim); err == nil {
		t.Fatal("restarting a node that is running should fail")
	}
}
