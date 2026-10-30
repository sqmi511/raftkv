package raft_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/raft/raftest"
	"github.com/sqmi511/raftkv/internal/store"
)

// InstallSnapshot makes nopTransport (declared in raft_test.go) satisfy the
// Transport interface.
func (nopTransport) InstallSnapshot(context.Context, string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, errors.New("nop transport")
}

// ---- a state machine that supports snapshots --------------------------------

type smachine struct {
	mu       sync.Mutex
	kv       map[string]string
	applies  int
	restores int
}

func newSMachine() *smachine { return &smachine{kv: map[string]string{}} }

func (m *smachine) Apply(e raft.Entry) {
	cmd, err := store.DecodeCommand(e.Data)
	if err != nil {
		panic(fmt.Sprintf("test machine got an undecodable entry %+v: %v", e, err))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applies++
	switch cmd.Op {
	case store.OpSet:
		m.kv[cmd.Key] = string(cmd.Value)
	case store.OpDelete:
		delete(m.kv, cmd.Key)
	}
}

func (m *smachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(m.kv)
}

func (m *smachine) Restore(b []byte) error {
	kv := map[string]string{}
	if err := json.Unmarshal(b, &kv); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv = kv
	m.restores++
	return nil
}

func (m *smachine) contents() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.kv))
	for k, v := range m.kv {
		out[k] = v
	}
	return out
}

func (m *smachine) counts() (applies, restores int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applies, m.restores
}

// ---- cluster harness -----------------------------------------------------------

type scluster struct {
	t         *testing.T
	net       *raftest.Network
	ids       []string
	open      func(id string) raft.Storage
	threshold uint64
	nodes     map[string]*raft.Node
	stores    map[string]raft.Storage
	machines  map[string]*smachine
	down      map[string]bool
	seed      int64
}

func newSCluster(t *testing.T, size int, threshold uint64, open func(id string) raft.Storage) *scluster {
	t.Helper()
	c := &scluster{
		t:         t,
		net:       raftest.NewNetwork(),
		open:      open,
		threshold: threshold,
		nodes:     make(map[string]*raft.Node),
		stores:    make(map[string]raft.Storage),
		machines:  make(map[string]*smachine),
		down:      make(map[string]bool),
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
// empty state machine.
func (c *scluster) startNode(id string) {
	c.t.Helper()
	st := c.open(id)
	m := newSMachine()
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
		Snapshot:           m.Snapshot,
		Restore:            m.Restore,
		SnapshotThreshold:  c.threshold,
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
func (c *scluster) stopNode(id string) {
	if n := c.nodes[id]; n != nil {
		n.Stop()
	}
	if s := c.stores[id]; s != nil {
		_ = s.Close()
	}
	c.net.Isolate(id)
	c.down[id] = true
}

func (c *scluster) isolate(id string) { c.net.Isolate(id); c.down[id] = true }
func (c *scluster) heal(id string)    { c.net.Heal(id); delete(c.down, id) }

func (c *scluster) live() []string {
	var out []string
	for _, id := range c.ids {
		if !c.down[id] {
			out = append(out, id)
		}
	}
	return out
}

func (c *scluster) follower(leader string) string {
	for _, id := range c.ids {
		if id != leader {
			return id
		}
	}
	return ""
}

func (c *scluster) dump() string {
	var b strings.Builder
	for _, id := range c.ids {
		applies, restores := c.machines[id].counts()
		fmt.Fprintf(&b, "%s down=%v applies=%d restores=%d keys=%d status=%+v\n",
			id, c.down[id], applies, restores, len(c.machines[id].contents()), c.nodes[id].Status())
	}
	return b.String()
}

func (c *scluster) waitFor(desc string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s\n%s", desc, c.dump())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *scluster) waitForLeader() string {
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

// put proposes a write on the leader and records it in want.
func (c *scluster) put(leader string, want map[string]string, key, val string) {
	c.t.Helper()
	if _, _, err := c.nodes[leader].Propose(setCmd(key, val)); err != nil {
		c.t.Fatalf("Propose(%s) on %s: %v", key, leader, err)
	}
	want[key] = val
}

func (c *scluster) waitConverged(ids []string, want map[string]string) {
	c.t.Helper()
	c.waitFor(fmt.Sprintf("%v to hold exactly the %d expected keys", ids, len(want)), func() bool {
		for _, id := range ids {
			if !reflect.DeepEqual(c.machines[id].contents(), want) {
				return false
			}
		}
		return true
	})
}

// waitSnapshotted waits until the node has compacted its log at least once
// and has settled (fewer than `threshold` applied entries past the snapshot).
func (c *scluster) waitSnapshotted(id string) {
	c.t.Helper()
	c.waitFor("node "+id+" to snapshot", func() bool {
		s := c.nodes[id].Status()
		return s.SnapshotIndex > 0 && s.Applied-s.SnapshotIndex < c.threshold
	})
}

// ---- snapshot tests -------------------------------------------------------------

func TestSnapshotRequiresSnapshotFunction(t *testing.T) {
	_, err := raft.NewNode(raft.Config{ID: "a", Transport: nopTransport{}, SnapshotThreshold: 5})
	if !errors.Is(err, raft.ErrInvalidConfig) {
		t.Fatalf("NewNode = %v, want ErrInvalidConfig", err)
	}
}

func TestLogIsCompactedAfterThreshold(t *testing.T) {
	st := raft.NewMemStorage()
	c := newSCluster(t, 1, 10, func(string) raft.Storage { return st })
	leader := c.waitForLeader()

	want := map[string]string{}
	for i := 0; i < 35; i++ {
		c.put(leader, want, fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	c.waitConverged(c.ids, want)
	c.waitSnapshotted(leader)

	s := c.nodes[leader].Status()
	if got, wantLen := len(st.Entries()), int(s.LastIndex-s.SnapshotIndex); got != wantLen {
		t.Fatalf("storage holds %d log entries, want %d (only those after the snapshot)", got, wantLen)
	}
	if snap := st.Snapshot(); snap.Index != s.SnapshotIndex || len(snap.Data) == 0 {
		t.Fatalf("stored snapshot = index %d with %d bytes; status says index %d", snap.Index, len(snap.Data), s.SnapshotIndex)
	}
}

func restartFromSnapshot(t *testing.T, open func(id string) raft.Storage) {
	t.Helper()
	c := newSCluster(t, 1, 10, open)
	id := c.ids[0]
	c.waitForLeader()

	want := map[string]string{}
	for i := 0; i < 35; i++ {
		c.put(id, want, fmt.Sprintf("k%d", i), fmt.Sprint(i))
	}
	c.waitConverged(c.ids, want)
	c.waitSnapshotted(id)
	snapIndex := c.nodes[id].Status().SnapshotIndex

	c.stopNode(id)
	c.startNode(id) // new process, same disk, EMPTY state machine
	c.waitForLeader()
	c.waitConverged([]string{id}, want)

	applies, restores := c.machines[id].counts()
	if restores != 1 {
		t.Fatalf("state machine was restored %d times, want exactly 1", restores)
	}
	if applies >= 35 {
		t.Fatalf("state machine replayed %d entries after restart: the snapshot was ignored", applies)
	}
	if got := c.nodes[id].Status().SnapshotIndex; got < snapIndex {
		t.Fatalf("snapshot index went backwards across the restart: %d -> %d", snapIndex, got)
	}

	// The restarted node keeps working.
	c.put(id, want, "after", "restart")
	c.waitConverged([]string{id}, want)
}

func TestRestartFromSnapshotWithMemoryStorage(t *testing.T) {
	restartFromSnapshot(t, memStorages())
}

func TestRestartFromSnapshotWithWAL(t *testing.T) {
	restartFromSnapshot(t, walStorages(t))
}

func TestLaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	c := newSCluster(t, 3, 10, memStorages())
	leader := c.waitForLeader()
	lagging := c.follower(leader)

	want := map[string]string{}
	c.put(leader, want, "before", "1")
	c.waitConverged(c.ids, want)

	c.isolate(lagging)
	for i := 0; i < 60; i++ {
		c.put(leader, want, fmt.Sprintf("k%d", i), "v")
	}
	c.waitConverged(c.live(), want)
	c.waitFor("the leader to compact past the lagging follower's log", func() bool {
		return c.nodes[leader].Status().SnapshotIndex > c.nodes[lagging].Status().LastIndex
	})

	// The entries the follower needs no longer exist anywhere: only a
	// snapshot can bring it up to date.
	c.heal(lagging)
	c.waitConverged(c.ids, want)
	if _, restores := c.machines[lagging].counts(); restores == 0 {
		t.Fatal("the follower caught up without restoring a snapshot")
	}
	if c.nodes[lagging].Status().SnapshotIndex == 0 {
		t.Fatal("the follower has no snapshot after InstallSnapshot")
	}

	// Normal replication continues afterwards.
	cur := c.waitForLeader()
	c.put(cur, want, "after", "x")
	c.waitConverged(c.ids, want)
}

func TestRestartedFollowerRestoresItsSnapshotThenCatchesUp(t *testing.T) {
	c := newSCluster(t, 3, 10, memStorages())
	leader := c.waitForLeader()
	f := c.follower(leader)

	want := map[string]string{}
	for i := 0; i < 30; i++ {
		c.put(leader, want, fmt.Sprintf("a%d", i), "v")
	}
	c.waitConverged(c.ids, want)
	c.waitSnapshotted(f)

	c.stopNode(f)
	for i := 0; i < 40; i++ {
		c.put(leader, want, fmt.Sprintf("b%d", i), "v")
	}
	c.waitConverged(c.live(), want)

	c.startNode(f) // restores its own snapshot, then catches up from the leader
	c.waitConverged(c.ids, want)
	if _, restores := c.machines[f].counts(); restores == 0 {
		t.Fatal("the restarted follower never restored a snapshot")
	}

	// Leadership can move to a node that was restored from a snapshot.
	cur := c.waitForLeader()
	c.stopNode(cur)
	next := c.waitForLeader()
	c.put(next, want, "final", "y")
	c.waitConverged(c.live(), want)
}

// ---- InstallSnapshot / AppendEntries handlers on a bare node --------------------

func fiveEntries() []raft.Entry {
	return []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c"), ent(4, 1, "d"), ent(5, 1, "e")}
}

func TestInstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: fiveEntries()})

	r := n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 1, LeaderID: "a", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("snap"),
	})
	if !r.Success || r.Term != 1 {
		t.Fatalf("reply = %+v, want success at term 1", r)
	}
	if s := n.Status(); s.SnapshotIndex != 3 || s.CommitIndex != 3 || s.LastIndex != 5 {
		t.Fatalf("status = %+v, want snapshot 3, commit 3, last 5", s)
	}
	if got := st.Entries(); len(got) != 2 || got[0].Index != 4 || got[1].Index != 5 {
		t.Fatalf("persisted log = %+v, want entries 4 and 5", got)
	}
	if snap := st.Snapshot(); snap.Index != 3 || string(snap.Data) != "snap" {
		t.Fatalf("persisted snapshot = %+v", snap)
	}
}

func TestInstallSnapshotDiscardsConflictingLog(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: fiveEntries()})

	// The snapshot says entry 4 was written in term 2; ours was term 1.
	r := n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 2, LeaderID: "b", LastIncludedIndex: 4, LastIncludedTerm: 2, Data: []byte("snap"),
	})
	if !r.Success {
		t.Fatalf("reply = %+v, want success", r)
	}
	if s := n.Status(); s.SnapshotIndex != 4 || s.LastIndex != 4 || s.CommitIndex != 4 {
		t.Fatalf("status = %+v, want everything at 4", s)
	}
	if got := st.Entries(); len(got) != 0 {
		t.Fatalf("persisted log = %+v, want empty", got)
	}
}

func TestInstallSnapshotIgnoresWhatWeAlreadyHave(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", LeaderCommit: 3, Entries: fiveEntries()[:3],
	})

	for _, idx := range []uint64{2, 3} { // at or below our commit index
		r := n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
			Term: 1, LeaderID: "a", LastIncludedIndex: idx, LastIncludedTerm: 1, Data: []byte("old"),
		})
		if !r.Success {
			t.Fatalf("idx %d: reply = %+v, want success (we hold everything it covers)", idx, r)
		}
	}
	if s := n.Status(); s.SnapshotIndex != 0 || s.LastIndex != 3 {
		t.Fatalf("status = %+v, want the log untouched", s)
	}
	if st.Snapshot().Index != 0 {
		t.Fatal("a stale snapshot was persisted")
	}
}

func TestInstallSnapshotRejectsStaleTermAndBadIndex(t *testing.T) {
	n := newBareNodeWith(t, raft.NewMemStorage())
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 5, LeaderID: "a"})

	r := n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 4, LeaderID: "b", LastIncludedIndex: 10, LastIncludedTerm: 4,
	})
	if r.Success || r.Term != 5 {
		t.Fatalf("stale-term reply = %+v, want rejection with term 5", r)
	}
	r = n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{Term: 5, LeaderID: "a"})
	if r.Success {
		t.Fatalf("index-0 snapshot reply = %+v, want rejection", r)
	}
	if n.Status().SnapshotIndex != 0 {
		t.Fatal("a rejected snapshot changed the node")
	}
}

func TestAppendEntriesBelowOurSnapshotIsTrimmed(t *testing.T) {
	st := raft.NewMemStorage()
	n := newBareNodeWith(t, st)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "a", Entries: fiveEntries()})
	n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 1, LeaderID: "a", LastIncludedIndex: 3, LastIncludedTerm: 1, Data: []byte("snap"),
	})

	// The leader starts at index 2, which we have compacted: the overlapping
	// part must be skipped and entry 6 appended.
	r := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{ent(2, 1, "b"), ent(3, 1, "c"), ent(4, 1, "d"), ent(5, 1, "e"), ent(6, 1, "f")},
	})
	if !r.Success {
		t.Fatalf("reply = %+v, want success", r)
	}
	if s := n.Status(); s.LastIndex != 6 || s.SnapshotIndex != 3 {
		t.Fatalf("status = %+v, want last 6 above snapshot 3", s)
	}
	if got := st.Entries(); len(got) != 3 || got[2].Index != 6 {
		t.Fatalf("persisted log = %+v, want entries 4..6", got)
	}

	// An RPC that ends entirely inside the snapshot changes nothing.
	r = n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "a", PrevLogIndex: 0, Entries: []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")},
	})
	if !r.Success || n.Status().LastIndex != 6 {
		t.Fatalf("reply = %+v, last = %d; want success and an unchanged log", r, n.Status().LastIndex)
	}
}
