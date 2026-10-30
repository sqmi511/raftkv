package raft_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/raft/raftest"
	"github.com/sqmi511/raftkv/internal/store"
)

// ---- test state machine -----------------------------------------------------

// machine applies store commands to an in-memory store and remembers every
// entry it was given, so tests can compare what each node applied.
type machine struct {
	mu      sync.Mutex
	kv      *store.Store
	applied []raft.Entry
}

func newMachine() *machine {
	return &machine{kv: store.New(store.WithSweepInterval(0))}
}

func (m *machine) Apply(e raft.Entry) {
	cmd, err := store.DecodeCommand(e.Data)
	if err != nil {
		panic(fmt.Sprintf("test machine got an undecodable entry %+v: %v", e, err))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, e)
	switch cmd.Op {
	case store.OpSet:
		m.kv.Set(cmd.Key, cmd.Value, 0)
	case store.OpDelete:
		m.kv.Delete(cmd.Key)
	}
}

func (m *machine) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.applied)
}

func (m *machine) entries() []raft.Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]raft.Entry(nil), m.applied...)
}

func (m *machine) get(key string) (string, bool) {
	v, ok := m.kv.Get(key)
	return string(v), ok
}

// entry returns the applied entry that has the given log index.
func (m *machine) entry(index uint64) (raft.Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.applied {
		if e.Index == index {
			return e, true
		}
	}
	return raft.Entry{}, false
}

func setCmd(key, val string) []byte {
	return store.Command{Op: store.OpSet, Key: key, Value: []byte(val)}.Encode()
}

func ent(index, term uint64, data string) raft.Entry {
	return raft.Entry{Index: index, Term: term, Data: []byte(data)}
}

func eventually(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- single-node unit tests -------------------------------------------------

func newBareNodeWith(t *testing.T, st raft.Storage) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(raft.Config{
		ID: "me", Peers: []string{"a", "b"}, Transport: nopTransport{}, Storage: st,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAppendEntriesConsistencyCheck(t *testing.T) {
	n := newBareNodeWith(t, raft.NewMemStorage())

	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")},
	})
	if !r.Success || n.Status().LastIndex != 2 {
		t.Fatalf("initial append = %+v, LastIndex = %d; want success and 2", r, n.Status().LastIndex)
	}

	// The leader's previous entry lies beyond our log: hint where our log ends.
	r = n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", PrevLogIndex: 5, PrevLogTerm: 1})
	if r.Success || r.ConflictIndex != 3 {
		t.Fatalf("prev beyond log = %+v, want rejection with ConflictIndex 3", r)
	}

	// The previous entry exists with a different term: hint at the first
	// index of our conflicting term.
	r = n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", PrevLogIndex: 2, PrevLogTerm: 9})
	if r.Success || r.ConflictIndex != 1 {
		t.Fatalf("term mismatch = %+v, want rejection with ConflictIndex 1", r)
	}
}

func TestAppendEntriesReplacesConflictingSuffix(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)

	n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a",
		Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")},
	})

	// A new leader in term 2 disagrees about everything after index 1.
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{ent(2, 2, "new2")},
	})
	if !r.Success {
		t.Fatalf("append = %+v, want success", r)
	}
	got := st.Entries()
	if len(got) != 2 || got[1].Term != 2 || string(got[1].Data) != "new2" {
		t.Fatalf("persisted log = %+v, want [1:t1, 2:t2 new2]", got)
	}
	if n.Status().LastIndex != 2 {
		t.Fatalf("LastIndex = %d, want 2", n.Status().LastIndex)
	}
}

func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	all := []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")}

	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: all})

	// A delayed duplicate that carries only a prefix of the log arrives late.
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: all[:1]})
	if !r.Success {
		t.Fatalf("duplicate = %+v, want success", r)
	}
	if got := len(st.Entries()); got != 3 || n.Status().LastIndex != 3 {
		t.Fatalf("log has %d entries (LastIndex %d), want 3", got, n.Status().LastIndex)
	}
}

func TestCommitIndexIsBoundedByVerifiedEntries(t *testing.T) {
	n := newBareNodeWith(t, raft.NewMemStorage())
	n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a",
		Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")},
	})

	// The leader only verified entry 1 against our log, so even though it
	// says "3 is committed" we must not commit entries 2 and 3 yet.
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3,
	})
	if !r.Success {
		t.Fatalf("heartbeat = %+v, want success", r)
	}
	if got := n.Status().CommitIndex; got != 1 {
		t.Fatalf("CommitIndex = %d, want 1", got)
	}
}

func TestFollowerNeverOverwritesCommittedEntries(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", LeaderCommit: 3,
		Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c")},
	})
	if got := n.Status().CommitIndex; got != 3 {
		t.Fatalf("CommitIndex = %d, want 3", got)
	}

	// A (buggy or malicious) leader tries to rewrite committed entry 2.
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{ent(2, 2, "evil")},
	})
	if r.Success {
		t.Fatal("follower accepted an overwrite of a committed entry")
	}
	got := st.Entries()
	if len(got) != 3 || got[1].Term != 1 || string(got[1].Data) != "b" {
		t.Fatalf("committed entries were modified: %+v", got)
	}
}

func TestRequestVoteRequiresUpToDateLog(t *testing.T) {
	n := newBareNodeWith(t, raft.NewMemStorage())
	// Our log ends with index 2 in term 2.
	n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: "a", Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 2, "b")},
	})

	cases := []struct {
		name      string
		lastIndex uint64
		lastTerm  uint64
		want      bool
	}{
		{"older last term, longer log", 5, 1, false},
		{"same last term, shorter log", 1, 2, false},
		{"same last term, same length", 2, 2, true},
		{"newer last term, shorter log", 1, 3, true},
	}
	for i, tc := range cases {
		r := n.HandleRequestVote(&raft.RequestVoteArgs{
			Term: uint64(3 + i), CandidateID: "c", LastLogIndex: tc.lastIndex, LastLogTerm: tc.lastTerm,
		})
		if r.VoteGranted != tc.want {
			t.Errorf("%s: VoteGranted = %v, want %v", tc.name, r.VoteGranted, tc.want)
		}
	}
}

func TestVoteAndTermSurviveRestart(t *testing.T) {
	st := raft.NewMemStorage()
	n1 := newBareNodeWith(t, st)
	if r := n1.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "a"}); !r.VoteGranted {
		t.Fatal("first vote should be granted")
	}

	n2 := newBareNodeWith(t, st) // "restart" with the same disk
	if got := n2.Status().Term; got != 5 {
		t.Fatalf("restarted node term = %d, want 5", got)
	}
	if r := n2.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "b"}); r.VoteGranted {
		t.Fatal("restarted node voted twice in the same term")
	}
	if r := n2.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "a"}); !r.VoteGranted {
		t.Fatal("restarted node should still honour its vote for a")
	}
}

func TestLogSurvivesRestart(t *testing.T) {
	st := raft.NewMemStorage()
	n1 := newBareNodeWith(t, st)
	n1.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")},
	})

	n2 := newBareNodeWith(t, st)
	if got := n2.Status().LastIndex; got != 2 {
		t.Fatalf("restarted node LastIndex = %d, want 2", got)
	}
}

func TestProposeGuards(t *testing.T) {
	n := newBareNodeWith(t, raft.NewMemStorage())

	if _, _, err := n.Propose(nil); !errors.Is(err, raft.ErrEmptyCommand) {
		t.Fatalf("empty Propose = %v, want ErrEmptyCommand", err)
	}
	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("Propose on a follower = %v, want ErrNotLeader", err)
	}
	n.Stop()
	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, raft.ErrStopped) {
		t.Fatalf("Propose after Stop = %v, want ErrStopped", err)
	}
}

func TestSingleNodeCommitsAndApplies(t *testing.T) {
	m := newMachine()
	n, err := raft.NewNode(raft.Config{
		ID:                 "solo",
		Transport:          nopTransport{},
		Apply:              m.Apply,
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(n.Stop)

	eventually(t, "the single node to become leader", func() bool { return n.Status().State == raft.Leader })

	idx, _, err := n.Propose(setCmd("k", "v"))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the write to be applied", func() bool { return m.count() == 1 })

	if st := n.Status(); st.CommitIndex < idx || st.Applied < idx {
		t.Fatalf("status %+v, want commit and applied >= %d", st, idx)
	}
	if v, ok := m.get("k"); !ok || v != "v" {
		t.Fatalf("Get(k) = %q, %v; want v", v, ok)
	}
}

// ---- cluster test harness ---------------------------------------------------

type rcluster struct {
	t        *testing.T
	net      *raftest.Network
	ids      []string
	open     func(id string) raft.Storage
	nodes    map[string]*raft.Node
	stores   map[string]raft.Storage
	machines map[string]*machine
	down     map[string]bool // isolated, stopped or failed: excluded from "live"
	leaders  map[uint64]string
	seed     int64
}

func memStorages() func(id string) raft.Storage {
	stores := map[string]*raft.MemStorage{}
	return func(id string) raft.Storage {
		s, ok := stores[id]
		if !ok {
			s = raft.NewMemStorage()
			stores[id] = s
		}
		return s
	}
}

func walStorages(t *testing.T) func(id string) raft.Storage {
	dir := t.TempDir()
	return func(id string) raft.Storage {
		s, err := raft.OpenWALStorage(filepath.Join(dir, id+".wal"))
		if err != nil {
			t.Fatalf("OpenWALStorage(%s): %v", id, err)
		}
		return s
	}
}

func newRCluster(t *testing.T, size int, open func(id string) raft.Storage) *rcluster {
	t.Helper()
	c := &rcluster{
		t:        t,
		net:      raftest.NewNetwork(),
		open:     open,
		nodes:    make(map[string]*raft.Node),
		stores:   make(map[string]raft.Storage),
		machines: make(map[string]*machine),
		down:     make(map[string]bool),
		leaders:  make(map[uint64]string),
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stopNode(id)
		}
	})
	return c
}

// startNode (re)creates a node from whatever its storage holds, with a fresh,
// empty state machine, and connects it to the network.
func (c *rcluster) startNode(id string) {
	c.t.Helper()
	st := c.open(id)
	m := newMachine()
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	c.seed++
	n, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              peers,
		Transport:          c.net.Transport(id),
		Storage:            st,
		Apply:              m.Apply,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		Seed:               c.seed,
	})
	if err != nil {
		c.t.Fatalf("NewNode(%s): %v", id, err)
	}
	c.stores[id], c.machines[id], c.nodes[id] = st, m, n
	c.net.Register(id, n)
	c.net.Heal(id)
	delete(c.down, id)
	n.Start()
}

// stopNode simulates a crash: the node stops and becomes unreachable, but its
// storage keeps whatever it had persisted.
func (c *rcluster) stopNode(id string) {
	if n := c.nodes[id]; n != nil {
		n.Stop()
	}
	if s := c.stores[id]; s != nil {
		_ = s.Close()
	}
	c.net.Isolate(id)
	c.down[id] = true
}

func (c *rcluster) isolate(id string) { c.net.Isolate(id); c.down[id] = true }
func (c *rcluster) heal(id string)    { c.net.Heal(id); delete(c.down, id) }

func (c *rcluster) live() []string {
	var out []string
	for _, id := range c.ids {
		if !c.down[id] {
			out = append(out, id)
		}
	}
	return out
}

func (c *rcluster) follower(leader string) string {
	for _, id := range c.ids {
		if id != leader {
			return id
		}
	}
	return ""
}

// snapshot enforces election safety: one leader per term, ever.
func (c *rcluster) snapshot() {
	c.t.Helper()
	for id, n := range c.nodes {
		s := n.Status()
		if s.State != raft.Leader {
			continue
		}
		if prev, ok := c.leaders[s.Term]; ok && prev != id {
			c.t.Fatalf("election safety violated: %s and %s were both leader in term %d", prev, id, s.Term)
		}
		c.leaders[s.Term] = id
	}
}

func (c *rcluster) dump() string {
	var b strings.Builder
	for _, id := range c.ids {
		fmt.Fprintf(&b, "%s down=%v applied=%d status=%+v\n",
			id, c.down[id], c.machines[id].count(), c.nodes[id].Status())
	}
	return b.String()
}

func (c *rcluster) waitFor(desc string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		c.snapshot()
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s\n%s", desc, c.dump())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForLeader waits until the live nodes agree on a term and exactly one of
// them is its leader. It returns that leader's ID.
func (c *rcluster) waitForLeader() string {
	c.t.Helper()
	var leader string
	c.waitFor("a single leader", func() bool {
		leader = ""
		var term uint64
		live := c.live()
		for _, id := range live {
			if s := c.nodes[id].Status(); s.State == raft.Leader {
				if leader != "" {
					return false
				}
				leader, term = id, s.Term
			}
		}
		if leader == "" {
			return false
		}
		for _, id := range live {
			if c.nodes[id].Status().Term != term {
				return false
			}
		}
		return true
	})
	return leader
}

func (c *rcluster) propose(id string, cmd []byte) (index, term uint64) {
	c.t.Helper()
	i, tm, err := c.nodes[id].Propose(cmd)
	if err != nil {
		c.t.Fatalf("Propose on %s: %v", id, err)
	}
	return i, tm
}

func (c *rcluster) waitApplied(ids []string, n int) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("%d applied entries on %v", n, ids), func() bool {
		for _, id := range ids {
			if c.machines[id].count() < n {
				return false
			}
		}
		return true
	})
}

func (c *rcluster) expectValue(id, key, want string) {
	c.t.Helper()
	if got, ok := c.machines[id].get(key); !ok || got != want {
		c.t.Fatalf("%s: Get(%q) = %q, %v; want %q", id, key, got, ok, want)
	}
}

// checkAgreement verifies state machine safety: every node applied the same
// entries in the same order (shorter sequences are prefixes of the longest).
func (c *rcluster) checkAgreement() {
	c.t.Helper()
	var longest []raft.Entry
	var longestID string
	seqs := make(map[string][]raft.Entry, len(c.ids))
	for _, id := range c.ids {
		seqs[id] = c.machines[id].entries()
		if len(seqs[id]) > len(longest) {
			longest, longestID = seqs[id], id
		}
	}
	for id, seq := range seqs {
		for i, e := range seq {
			w := longest[i]
			if e.Index != w.Index || e.Term != w.Term || !bytes.Equal(e.Data, w.Data) {
				c.t.Fatalf("state machines diverged at position %d: %s applied %+v, %s applied %+v",
					i, id, e, longestID, w)
			}
		}
	}
}

// ---- replication tests --------------------------------------------------------

func TestReplicatesToAllNodes(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()

	const n = 20
	for i := 0; i < n; i++ {
		c.propose(leader, setCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(c.ids, n)
	c.checkAgreement()

	for _, id := range c.ids {
		for i := 0; i < n; i++ {
			c.expectValue(id, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		}
	}
}

func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()
	lagging := c.follower(leader)

	c.propose(leader, setCmd("before", "1"))
	c.waitApplied(c.ids, 1)

	c.isolate(lagging)
	const n = 600 // more than two AppendEntries batches
	for i := 0; i < n; i++ {
		c.propose(leader, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitApplied(c.live(), n+1) // the majority commits without the lagging node

	if got := c.machines[lagging].count(); got != 1 {
		t.Fatalf("isolated follower applied %d entries, want 1", got)
	}

	c.heal(lagging)
	c.waitApplied(c.ids, n+1)
	c.checkAgreement()
	c.expectValue(lagging, "k599", "v")
}

func TestLeaderChangeKeepsCommittedAndDropsUncommitted(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	old := c.waitForLeader()
	for i := 0; i < 10; i++ {
		c.propose(old, setCmd(fmt.Sprintf("a%d", i), "x"))
	}
	c.waitApplied(c.ids, 10)

	c.isolate(old)
	// The partitioned leader still believes it leads, so it accepts a write
	// that can never reach a quorum.
	c.propose(old, setCmd("doomed", "x"))

	next := c.waitForLeader()
	for i := 0; i < 10; i++ {
		c.propose(next, setCmd(fmt.Sprintf("b%d", i), "y"))
	}
	c.waitApplied(c.live(), 20)

	c.heal(old)
	c.waitApplied(c.ids, 20)
	c.checkAgreement()

	for _, id := range c.ids {
		if _, ok := c.machines[id].get("doomed"); ok {
			t.Fatalf("%s applied a write that was never committed", id)
		}
		c.expectValue(id, "a0", "x")
		c.expectValue(id, "b9", "y")
	}
}

func TestNoCommittedWriteIsEverLost(t *testing.T) {
	c := newRCluster(t, 5, memStorages())

	var acked, doomed []string
	for round := 0; round < 4; round++ {
		leader := c.waitForLeader()

		// Writes the leader has applied are committed: they must survive
		// everything that follows.
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("r%d-w%d", round, i)
			idx, term := c.propose(leader, setCmd(key, "v"))
			c.waitFor("commit of "+key, func() bool {
				e, ok := c.machines[leader].entry(idx)
				return ok && e.Term == term
			})
			acked = append(acked, key)
		}

		// Partition the leader away. It keeps accepting writes it cannot commit.
		c.isolate(leader)
		lost := fmt.Sprintf("r%d-lost", round)
		c.propose(leader, setCmd(lost, "v"))
		doomed = append(doomed, lost)

		c.waitForLeader() // the survivors elect a replacement
		c.heal(leader)
	}

	final := c.waitForLeader()
	c.propose(final, setCmd("final", "v"))
	acked = append(acked, "final")
	c.waitApplied(c.ids, len(acked))
	c.checkAgreement()

	for _, id := range c.ids {
		for _, k := range acked {
			if _, ok := c.machines[id].get(k); !ok {
				t.Fatalf("%s lost committed write %q", id, k)
			}
		}
		for _, k := range doomed {
			if _, ok := c.machines[id].get(k); ok {
				t.Fatalf("%s applied %q, which was never committed", id, k)
			}
		}
	}
}

func TestRestartedFollowerRecoversFromStorage(t *testing.T) {
	c := newRCluster(t, 3, memStorages())
	leader := c.waitForLeader()
	for i := 0; i < 5; i++ {
		c.propose(leader, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitApplied(c.ids, 5)

	f := c.follower(leader)
	c.stopNode(f)
	for i := 5; i < 10; i++ {
		c.propose(leader, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitApplied(c.live(), 10)

	c.startNode(f) // new process, same disk, empty state machine
	if st := c.nodes[f].Status(); st.LastIndex < 5 || st.Term < 1 {
		t.Fatalf("restarted node did not recover its state from storage: %+v", st)
	}
	c.waitApplied(c.ids, 10)
	c.checkAgreement()
	c.expectValue(f, "k9", "v")
}

func fullClusterRestart(t *testing.T, open func(id string) raft.Storage) {
	t.Helper()
	c := newRCluster(t, 3, open)
	leader := c.waitForLeader()

	const n = 10
	for i := 0; i < n; i++ {
		c.propose(leader, setCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(c.ids, n)
	termBefore := c.nodes[leader].Status().Term

	for _, id := range c.ids {
		c.stopNode(id)
	}
	for _, id := range c.ids {
		c.startNode(id)
	}

	leader = c.waitForLeader()
	if got := c.nodes[leader].Status().Term; got <= termBefore {
		t.Fatalf("term went from %d to %d across a restart; it must never go backwards", termBefore, got)
	}
	// The state machines are empty again: they are rebuilt from the
	// persisted logs once the new leader's no-op commits.
	c.waitApplied(c.ids, n)
	for _, id := range c.ids {
		for i := 0; i < n; i++ {
			c.expectValue(id, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		}
	}

	c.propose(leader, setCmd("after", "restart"))
	c.waitApplied(c.ids, n+1)
	c.checkAgreement()
}

func TestFullClusterRestartFromMemoryStorage(t *testing.T) {
	fullClusterRestart(t, memStorages())
}

func TestFullClusterRestartFromWAL(t *testing.T) {
	fullClusterRestart(t, walStorages(t))
}

func TestStorageFailureTakesNodeOutOfService(t *testing.T) {
	stores := map[string]*raft.MemStorage{}
	open := func(id string) raft.Storage {
		s := raft.NewMemStorage()
		stores[id] = s
		return s
	}
	c := newRCluster(t, 3, open)
	leader := c.waitForLeader()
	c.propose(leader, setCmd("k0", "v"))
	c.waitApplied(c.ids, 1)

	// The leader's disk starts failing: it must stop rather than
	// acknowledge anything it cannot make durable.
	stores[leader].SetError(errors.New("disk on fire"))
	if _, _, err := c.nodes[leader].Propose(setCmd("k1", "v")); err == nil {
		t.Fatal("Propose succeeded although the log could not be persisted")
	}
	if c.nodes[leader].Status().Err == nil {
		t.Fatal("node did not record its storage failure")
	}
	if _, _, err := c.nodes[leader].Propose(setCmd("k2", "v")); err == nil {
		t.Fatal("a failed node accepted another write")
	}

	c.down[leader] = true
	next := c.waitForLeader() // the failed leader stops heartbeating, so the others elect a new one
	if next == leader {
		t.Fatal("failed node is still leader")
	}
	c.propose(next, setCmd("k3", "v"))
	c.waitApplied(c.live(), 2) // k0 and k3; the rejected writes never happened
}
