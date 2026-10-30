package raft_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/raft/raftest"
)

const (
	electionMin = 200 * time.Millisecond
	electionMax = 400 * time.Millisecond
	heartbeat   = 40 * time.Millisecond
	waitTimeout = 10 * time.Second
)

// ---- single-node unit tests -------------------------------------------------

type nopTransport struct{}

func (nopTransport) RequestVote(context.Context, string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, errors.New("nop transport")
}

func (nopTransport) AppendEntries(context.Context, string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, errors.New("nop transport")
}

// newBareNode returns a node that is never started, so tests can drive its
// RPC handlers directly and deterministically.
func newBareNode(t *testing.T) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(raft.Config{ID: "me", Peers: []string{"a", "b"}, Transport: nopTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConfigValidation(t *testing.T) {
	good := raft.Config{ID: "a", Peers: []string{"b", "c"}, Transport: nopTransport{}}
	if _, err := raft.NewNode(good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := map[string]func(*raft.Config){
		"missing id":        func(c *raft.Config) { c.ID = "" },
		"missing transport": func(c *raft.Config) { c.Transport = nil },
		"peer is self":      func(c *raft.Config) { c.Peers = []string{"a"} },
		"duplicate peer":    func(c *raft.Config) { c.Peers = []string{"b", "b"} },
		"empty peer":        func(c *raft.Config) { c.Peers = []string{""} },
		"max below min": func(c *raft.Config) {
			c.ElectionTimeoutMin = 300 * time.Millisecond
			c.ElectionTimeoutMax = 200 * time.Millisecond
		},
		"heartbeat not below election timeout": func(c *raft.Config) {
			c.HeartbeatInterval = time.Second
		},
	}
	for name, mutate := range cases {
		cfg := good
		mutate(&cfg)
		if _, err := raft.NewNode(cfg); !errors.Is(err, raft.ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestRequestVoteRules(t *testing.T) {
	n := newBareNode(t)

	// First candidate in a new term gets the vote.
	r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "a"})
	if !r.VoteGranted || r.Term != 5 {
		t.Fatalf("vote for a in term 5 = %+v, want granted at term 5", r)
	}
	// A stale term is rejected and told the current term.
	r = n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: "b"})
	if r.VoteGranted || r.Term != 5 {
		t.Fatalf("stale-term vote = %+v, want rejected with term 5", r)
	}
	// One vote per term: a different candidate in the same term is refused.
	r = n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "b"})
	if r.VoteGranted {
		t.Fatal("granted a second vote in the same term")
	}
	// The same candidate asking again (e.g. a retried RPC) is fine.
	r = n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "a"})
	if !r.VoteGranted {
		t.Fatal("refused a repeated request from the candidate we already voted for")
	}
	// A higher term clears the old vote.
	r = n.HandleRequestVote(&raft.RequestVoteArgs{Term: 6, CandidateID: "b"})
	if !r.VoteGranted || r.Term != 6 {
		t.Fatalf("vote for b in term 6 = %+v, want granted at term 6", r)
	}
	if got := n.Status().Term; got != 6 {
		t.Fatalf("node term = %d, want 6", got)
	}
}

func TestAppendEntriesRules(t *testing.T) {
	n := newBareNode(t)

	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 2, LeaderID: "a"})
	if !r.Success || r.Term != 2 {
		t.Fatalf("heartbeat = %+v, want success at term 2", r)
	}
	if st := n.Status(); st.State != raft.Follower || st.Term != 2 || st.LeaderID != "a" {
		t.Fatalf("status = %+v, want follower, term 2, leader a", st)
	}

	// A heartbeat from a deposed leader is rejected and told the new term.
	r = n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "b"})
	if r.Success || r.Term != 2 {
		t.Fatalf("stale heartbeat = %+v, want rejected with term 2", r)
	}
	if got := n.Status().LeaderID; got != "a" {
		t.Fatalf("leader changed to %q after a stale heartbeat", got)
	}
}

func TestSingleNodeClusterBecomesLeader(t *testing.T) {
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

	deadline := time.Now().Add(5 * time.Second)
	for n.Status().State != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("single node never became leader: %+v", n.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n.Status().Term < 1 {
		t.Fatal("leader term should be at least 1")
	}
}

func TestStopIsIdempotentAndFreezesNode(t *testing.T) {
	n := newBareNode(t)
	n.Start()
	n.Start() // second Start is a no-op
	n.Stop()
	n.Stop()
	n.Start() // Start after Stop is a no-op

	r := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 9, CandidateID: "a"})
	if r.VoteGranted {
		t.Fatal("stopped node granted a vote")
	}
	if got := n.Status().Term; got != 0 {
		t.Fatalf("stopped node changed term to %d", got)
	}
}

// ---- cluster tests ----------------------------------------------------------

type cluster struct {
	t            *testing.T
	net          *raftest.Network
	ids          []string
	nodes        map[string]*raft.Node
	isolated     map[string]bool
	leaderByTerm map[uint64]string
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{
		t:            t,
		net:          raftest.NewNetwork(),
		nodes:        make(map[string]*raft.Node),
		isolated:     make(map[string]bool),
		leaderByTerm: make(map[uint64]string),
	}
	for i := 0; i < size; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
	}
	for i, id := range c.ids {
		var peers []string
		for _, p := range c.ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		n, err := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          c.net.Transport(id),
			ElectionTimeoutMin: electionMin,
			ElectionTimeoutMax: electionMax,
			HeartbeatInterval:  heartbeat,
			Seed:               int64(i + 1),
		})
		if err != nil {
			t.Fatal(err)
		}
		c.net.Register(id, n)
		c.nodes[id] = n
	}
	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) isolate(id string) {
	c.net.Isolate(id)
	c.isolated[id] = true
}

func (c *cluster) heal(id string) {
	c.net.Heal(id)
	delete(c.isolated, id)
}

func (c *cluster) liveIDs() []string {
	var out []string
	for _, id := range c.ids {
		if !c.isolated[id] {
			out = append(out, id)
		}
	}
	return out
}

// snapshot reads every node's status and checks Raft's election safety
// property: at most one leader may ever exist in any single term.
func (c *cluster) snapshot() map[string]raft.Status {
	c.t.Helper()
	out := make(map[string]raft.Status, len(c.nodes))
	for id, n := range c.nodes {
		s := n.Status()
		out[id] = s
		if s.State == raft.Leader {
			if prev, ok := c.leaderByTerm[s.Term]; ok && prev != id {
				c.t.Fatalf("election safety violated: %s and %s were both leader in term %d", prev, id, s.Term)
			}
			c.leaderByTerm[s.Term] = id
		}
	}
	return out
}

func (c *cluster) waitFor(desc string, cond func(map[string]raft.Status) bool) map[string]raft.Status {
	c.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		st := c.snapshot()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s; state: %+v", desc, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForLeader waits until the non-isolated nodes agree on a term and
// exactly one of them is its leader. It returns that leader's ID.
func (c *cluster) waitForLeader() string {
	c.t.Helper()
	var leader string
	c.waitFor("a single leader", func(st map[string]raft.Status) bool {
		leader = ""
		var term uint64
		for _, id := range c.liveIDs() {
			if st[id].State == raft.Leader {
				if leader != "" {
					return false
				}
				leader, term = id, st[id].Term
			}
		}
		if leader == "" {
			return false
		}
		for _, id := range c.liveIDs() {
			if st[id].Term != term {
				return false
			}
		}
		return true
	})
	return leader
}

// observe samples the cluster for d, enforcing election safety throughout.
func (c *cluster) observe(d time.Duration, check func(map[string]raft.Status)) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		st := c.snapshot()
		if check != nil {
			check(st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()

	// Followers learn who the leader is from its heartbeats.
	c.waitFor("followers to learn the leader", func(st map[string]raft.Status) bool {
		for _, id := range c.ids {
			if st[id].LeaderID != leader {
				return false
			}
			if id != leader && st[id].State != raft.Follower {
				return false
			}
		}
		return true
	})
}

func TestLeaderIsStableWhileHeartbeatsFlow(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()
	term := c.nodes[leader].Status().Term

	c.observe(2*time.Second, func(st map[string]raft.Status) {
		if st[leader].State != raft.Leader || st[leader].Term != term {
			t.Fatalf("leadership changed without a failure: %+v", st)
		}
	})
}

func TestLeaderFailureTriggersReelection(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitForLeader()
	oldTerm := c.nodes[old].Status().Term

	c.isolate(old)
	replacement := c.waitForLeader() // among the two remaining nodes
	if replacement == old {
		t.Fatal("isolated node is still reported as leader")
	}
	newTerm := c.nodes[replacement].Status().Term
	if newTerm <= oldTerm {
		t.Fatalf("new leader term %d is not above old term %d", newTerm, oldTerm)
	}

	// The old leader returns. It must notice the higher term and step down.
	c.heal(old)
	c.waitForLeader() // all three agree on a term and have exactly one leader
	if got := c.nodes[old].Status().Term; got < newTerm {
		t.Fatalf("returned node is stuck at term %d, want >= %d", got, newTerm)
	}
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()

	var survivor, other string
	for _, id := range c.ids {
		switch {
		case id == leader:
		case survivor == "":
			survivor = id
		default:
			other = id
		}
	}
	startTerm := c.nodes[survivor].Status().Term

	// Leave the survivor alone: 1 of 3 nodes can never win an election.
	c.isolate(leader)
	c.isolate(other)

	c.observe(1500*time.Millisecond, func(st map[string]raft.Status) {
		if st[survivor].State == raft.Leader {
			t.Fatalf("%s became leader without a quorum: %+v", survivor, st[survivor])
		}
	})
	if got := c.nodes[survivor].Status().Term; got <= startTerm {
		t.Fatalf("survivor never started an election: term %d -> %d", startTerm, got)
	}

	// Once connectivity returns, the cluster converges on one leader even
	// though terms diverged during the partition.
	c.heal(leader)
	c.heal(other)
	c.waitForLeader()
}

func TestFiveNodesTolerateTwoFailures(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitForLeader()

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.isolate(leader)
	c.isolate(follower)

	replacement := c.waitForLeader() // 3 of 5 is still a majority
	if replacement == leader || replacement == follower {
		t.Fatalf("leader %s is one of the failed nodes", replacement)
	}
}

func TestRepeatedFailoversNeverProduceTwoLeadersInATerm(t *testing.T) {
	c := newCluster(t, 5)

	var lastTerm uint64
	for round := 0; round < 4; round++ {
		leader := c.waitForLeader()
		term := c.nodes[leader].Status().Term
		if term <= lastTerm {
			t.Fatalf("round %d: term %d did not advance past %d", round, term, lastTerm)
		}
		lastTerm = term

		c.isolate(leader)
		if next := c.waitForLeader(); next == leader {
			t.Fatalf("round %d: isolated leader %s was re-elected", round, leader)
		}
		c.heal(leader)
		c.observe(300*time.Millisecond, nil) // snapshot() enforces the invariant
	}
}
