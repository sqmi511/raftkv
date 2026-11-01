package shard_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/kv/kvtest"
	"github.com/sqmi511/raftkv/internal/shard"
)

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

func timeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// startGroups starts n independent three-node clusters named g0, g1, ...
func startGroups(t *testing.T, n int) ([]*kvtest.Cluster, []shard.Group) {
	t.Helper()
	clusters := make([]*kvtest.Cluster, n)
	groups := make([]shard.Group, n)
	for i := range clusters {
		clusters[i] = kvtest.NewCluster(t, 3)
		groups[i] = shard.Group{Name: fmt.Sprintf("g%d", i), Addrs: clusters[i].Addrs()}
	}
	for _, c := range clusters {
		c.WaitLeader()
	}
	return clusters, groups
}

func newShardClient(t *testing.T, groups []shard.Group) *shard.Client {
	t.Helper()
	sc, err := shard.New(groups, client.Options{MaxAttempts: 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc
}

func TestNewValidation(t *testing.T) {
	if _, err := shard.New(nil, client.Options{}); !errors.Is(err, shard.ErrEmptyRing) {
		t.Fatalf("New without groups = %v, want ErrEmptyRing", err)
	}
	dup := []shard.Group{{Name: "a", Addrs: []string{"x:1"}}, {Name: "a", Addrs: []string{"y:1"}}}
	if _, err := shard.New(dup, client.Options{}); !errors.Is(err, shard.ErrInvalidGroup) {
		t.Fatalf("New with duplicate names = %v, want ErrInvalidGroup", err)
	}
	if _, err := shard.New([]shard.Group{{Name: "a"}}, client.Options{}); err == nil {
		t.Fatal("a group without addresses was accepted")
	}
}

func TestKeysLandOnTheirOwnGroupOnly(t *testing.T) {
	clusters, groups := startGroups(t, 3)
	sc := newShardClient(t, groups)
	index := map[string]int{"g0": 0, "g1": 1, "g2": 2}
	ctx := timeout(t, 2*time.Minute)

	const n = 60
	perGroup := map[string]int{}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := sc.Set(ctx, key, []byte(key), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
		perGroup[sc.Locate(key)]++
	}
	for _, g := range groups {
		if perGroup[g.Name] == 0 {
			t.Fatalf("group %s received no keys: %v", g.Name, perGroup)
		}
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if v, ok, err := sc.Get(ctx, key); err != nil || !ok || string(v) != key {
			t.Fatalf("Get(%s) = %q, %v, %v", key, v, ok, err)
		}

		owner := index[sc.Locate(key)]
		for g, c := range clusters {
			for _, addr := range c.Addrs() {
				svc := c.Service(addr)
				if g == owner {
					eventually(t, "replication of "+key, func() bool {
						v, ok := svc.StaleGet(key)
						return ok && string(v) == key
					})
				} else if _, ok := svc.StaleGet(key); ok {
					t.Fatalf("key %s is stored on group g%d, but g%d owns it", key, g, owner)
				}
			}
		}
	}

	if deleted, err := sc.Delete(ctx, "key-0"); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v; want true", deleted, err)
	}
	if _, ok, _ := sc.Get(ctx, "key-0"); ok {
		t.Fatal("key-0 still present after Delete")
	}
}

func TestGroupWithoutQuorumDoesNotAffectOtherGroups(t *testing.T) {
	clusters, groups := startGroups(t, 2)
	sc := newShardClient(t, groups)

	var key0, key1 string
	for i := 0; key0 == "" || key1 == ""; i++ {
		k := fmt.Sprintf("probe-%d", i)
		switch sc.Locate(k) {
		case "g0":
			if key0 == "" {
				key0 = k
			}
		case "g1":
			if key1 == "" {
				key1 = k
			}
		}
	}
	ctx := timeout(t, 3*time.Minute)
	for _, k := range []string{key0, key1} {
		if err := sc.Set(ctx, k, []byte("before"), 0); err != nil {
			t.Fatal(err)
		}
	}

	// Take g0 below its majority: kill its leader and one more node.
	c0 := clusters[0]
	leader := c0.WaitLeader()
	other := c0.Followers(leader)[0]
	c0.Stop(leader)
	c0.Stop(other)

	// The other group does not notice.
	if err := sc.Set(timeout(t, 10*time.Second), key1, []byte("during"), 0); err != nil {
		t.Fatalf("a write to the healthy group failed: %v", err)
	}
	// The broken group refuses to commit anything.
	if err := sc.Set(timeout(t, 2*time.Second), key0, []byte("x"), 0); err == nil {
		t.Fatal("a write to a group without a majority succeeded")
	}

	// Bring the group back: it recovers and keeps its data.
	for _, addr := range []string{leader, other} {
		if err := c0.Restart(addr); err != nil {
			t.Fatal(err)
		}
	}
	c0.WaitLeader()
	if err := sc.Set(ctx, key0, []byte("after"), 0); err != nil {
		t.Fatalf("write to the recovered group: %v", err)
	}
	for k, want := range map[string]string{key0: "after", key1: "during"} {
		if v, ok, err := sc.Get(ctx, k); err != nil || !ok || string(v) != want {
			t.Fatalf("Get(%s) = %q, %v, %v; want %q", k, v, ok, err, want)
		}
	}
}
