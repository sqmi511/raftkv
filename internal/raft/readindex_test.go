package raft_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
)

func readCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// waitCommitted waits until the node's whole log is committed, which for a
// freshly elected leader means its no-op entry has committed.
func (c *rcluster) waitCommitted(id string) {
	c.t.Helper()
	c.waitFor("node "+id+" to commit its log", func() bool {
		st := c.nodes[id].Status()
		return st.LastIndex >= 1 && st.CommitIndex >= st.LastIndex
	})
}

func TestReadIndexRequiresLeadership(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()

	follower := c.follower(leader)
	if _, err := c.nodes[follower].ReadIndex(readCtx(t, 2*time.Second)); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("ReadIndex on a follower = %v, want ErrNotLeader", err)
	}
}

func TestReadIndexReflectsCommittedWrites(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()

	var last uint64
	for i := 0; i < 5; i++ {
		last, _ = c.propose(leader, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitApplied(c.ids, 5)

	idx, err := c.nodes[leader].ReadIndex(readCtx(t, 5*time.Second))
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if idx < last {
		t.Fatalf("ReadIndex = %d, want at least the last committed write %d", idx, last)
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	n, err := raft.NewNode(raft.Config{
		ID:                 "solo",
		Transport:          nopTransport{},
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(n.Stop)

	var idx uint64
	eventually(t, "ReadIndex to succeed on the single node", func() bool {
		i, err := n.ReadIndex(readCtx(t, 2*time.Second))
		idx = i
		return err == nil
	})
	if idx < 1 {
		t.Fatalf("ReadIndex = %d, want at least 1 (the leader's no-op)", idx)
	}
}

func TestReadIndexBlocksWithoutQuorum(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()
	c.waitCommitted(leader)

	// The leader is cut off but still believes it leads. It must NOT confirm
	// a read: the rest of the cluster may already have a newer leader.
	c.isolate(leader)

	_, err := c.nodes[leader].ReadIndex(readCtx(t, 300*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReadIndex on an isolated leader = %v, want a timeout", err)
	}
}

func TestReadIndexFailsWhenLeaderIsDeposed(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	old := c.waitForLeader()
	c.waitCommitted(old)

	c.isolate(old)
	errCh := make(chan error, 1)
	ctx := readCtx(t, 20*time.Second)
	go func() {
		_, err := c.nodes[old].ReadIndex(ctx)
		errCh <- err
	}()

	c.waitForLeader() // the other two elect a replacement
	c.heal(old)       // the old leader sees the higher term and steps down

	select {
	case err := <-errCh:
		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("pending read failed with %v, want ErrNotLeader", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("pending read was never released after the leader was deposed")
	}
}
