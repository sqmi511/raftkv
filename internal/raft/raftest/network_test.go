package raftest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/raft/raftest"
)

// newNodes creates unstarted nodes (their handlers work without Start) on a
// shared network.
func newNodes(t *testing.T, ids ...string) (*raftest.Network, map[string]*raft.Node) {
	t.Helper()
	nw := raftest.NewNetworkSeeded(1)
	nodes := make(map[string]*raft.Node)
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		n, err := raft.NewNode(raft.Config{ID: id, Peers: peers, Transport: nw.Transport(id)})
		if err != nil {
			t.Fatal(err)
		}
		nw.Register(id, n)
		nodes[id] = n
	}
	return nw, nodes
}

func vote(nw *raftest.Network, from, to string, term uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := nw.Transport(from).RequestVote(ctx, to, &raft.RequestVoteArgs{Term: term, CandidateID: from})
	return err
}

func TestDeliveryIsolationAndHeal(t *testing.T) {
	nw, _ := newNodes(t, "a", "b")

	if err := vote(nw, "a", "b", 1); err != nil {
		t.Fatalf("connected vote: %v", err)
	}
	nw.Isolate("b")
	if err := vote(nw, "a", "b", 2); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("vote to an isolated node = %v, want ErrUnreachable", err)
	}
	if err := vote(nw, "b", "a", 2); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("vote from an isolated node = %v, want ErrUnreachable", err)
	}
	nw.Heal("b")
	if err := vote(nw, "a", "b", 3); err != nil {
		t.Fatalf("vote after heal: %v", err)
	}
	if err := vote(nw, "a", "missing", 3); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("vote to an unknown node = %v, want ErrUnreachable", err)
	}
}

func TestPartitionGroups(t *testing.T) {
	nw, _ := newNodes(t, "a", "b", "c", "d")

	// a alone; b, c, d are not listed and stay together.
	nw.Partition([]string{"a"})
	if err := vote(nw, "a", "b", 1); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("a->b across the partition = %v, want ErrUnreachable", err)
	}
	if err := vote(nw, "c", "a", 1); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("c->a across the partition = %v, want ErrUnreachable", err)
	}
	if err := vote(nw, "b", "c", 1); err != nil {
		t.Fatalf("b->c inside the unlisted group: %v", err)
	}

	// Two explicit groups.
	nw.Partition([]string{"a", "b"}, []string{"c", "d"})
	if err := vote(nw, "a", "b", 2); err != nil {
		t.Fatalf("a->b inside a group: %v", err)
	}
	if err := vote(nw, "a", "c", 2); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("a->c across groups = %v, want ErrUnreachable", err)
	}

	nw.HealPartition()
	if err := vote(nw, "a", "c", 3); err != nil {
		t.Fatalf("a->c after HealPartition: %v", err)
	}
}

func TestDropIncludingLostReplies(t *testing.T) {
	nw, nodes := newNodes(t, "a", "b")

	nw.SetDropRate(1)
	if err := vote(nw, "a", "b", 1); !errors.Is(err, raftest.ErrDropped) {
		t.Fatalf("vote with drop rate 1 = %v, want ErrDropped", err)
	}
	if got := nodes["b"].Status().Term; got != 0 {
		t.Fatalf("a dropped request still reached the node (term %d)", got)
	}

	// At 50% some calls fail AFTER the handler ran: the work happened but
	// the caller never learns it.
	nw.SetDropRate(0.5)
	var ok, failed int
	replyLost := false
	for term := uint64(1); term <= 300; term++ {
		err := vote(nw, "a", "b", term)
		switch {
		case err == nil:
			ok++
		case errors.Is(err, raftest.ErrDropped):
			failed++
			if nodes["b"].Status().Term == term {
				replyLost = true
			}
		default:
			t.Fatalf("unexpected error %v", err)
		}
	}
	if ok == 0 || failed == 0 || !replyLost {
		t.Fatalf("ok=%d failed=%d replyLost=%v: want a mix, including at least one lost reply", ok, failed, replyLost)
	}

	nw.SetDropRate(0)
	if err := vote(nw, "a", "b", 1000); err != nil {
		t.Fatalf("vote after turning drops off: %v", err)
	}
}

func TestDelay(t *testing.T) {
	nw, _ := newNodes(t, "a", "b")
	nw.SetDelay(30*time.Millisecond, 30*time.Millisecond)

	start := time.Now()
	if err := vote(nw, "a", "b", 1); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 55*time.Millisecond {
		t.Fatalf("a delayed round trip took %v, want at least ~60ms (30ms each way)", took)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := nw.Transport("a").RequestVote(ctx, "b", &raft.RequestVoteArgs{Term: 2, CandidateID: "a"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("call whose deadline is shorter than the delay = %v, want DeadlineExceeded", err)
	}

	nw.SetDelay(0, 0)
	start = time.Now()
	if err := vote(nw, "a", "b", 3); err != nil || time.Since(start) > 25*time.Millisecond {
		t.Fatalf("undelayed vote: err=%v took=%v", err, time.Since(start))
	}
}

func TestResetFaultsKeepsIsolation(t *testing.T) {
	nw, _ := newNodes(t, "a", "b", "c")
	nw.Partition([]string{"a"})
	nw.SetDropRate(1)
	nw.SetDelay(time.Second, time.Second)
	nw.Isolate("c")

	nw.ResetFaults()
	if err := vote(nw, "a", "b", 1); err != nil {
		t.Fatalf("a->b after ResetFaults: %v", err)
	}
	if err := vote(nw, "a", "c", 1); !errors.Is(err, raftest.ErrUnreachable) {
		t.Fatalf("a->c = %v, want ErrUnreachable: ResetFaults must not heal isolation", err)
	}
}
