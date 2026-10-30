// Package raft implements the Raft consensus algorithm: leader election, log
// replication, commit tracking, linearizable read confirmation (ReadIndex),
// snapshots with log compaction, and persistence of term, vote, log and
// snapshot.
//
// A Node applies committed entries, in log order, from a single goroutine by
// calling Config.Apply. When a snapshot exists (after a restart, or when the
// leader sends one), Config.Restore is called first with its data, and
// Apply continues from the entry after it. Without a snapshot the state
// machine is rebuilt from the persisted log, so Apply starts at index 1 and
// the state machine must begin empty.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

var (
	// ErrInvalidConfig is wrapped by errors returned from NewNode.
	ErrInvalidConfig = errors.New("raft: invalid config")
	// ErrNotLeader is returned by Propose and ReadIndex on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrEmptyCommand is returned by Propose for an empty command.
	ErrEmptyCommand = errors.New("raft: empty command")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("raft: node stopped")
	// ErrNotReady is returned by ReadIndex while a new leader has not yet
	// committed an entry from its own term. Retry in a few milliseconds.
	ErrNotReady = errors.New("raft: leader has not committed an entry in its term yet")
)

const (
	// maxEntriesPerRPC bounds how many log entries one AppendEntries carries.
	maxEntriesPerRPC = 256
	// snapshotRPCTimeout bounds one InstallSnapshot RPC. Snapshots are sent
	// in a single message, so this is far more generous than for heartbeats.
	snapshotRPCTimeout = 10 * time.Second
)

// Config configures a Node.
type Config struct {
	ID        string    // this node's ID; must be unique in the cluster
	Peers     []string  // IDs of all OTHER nodes
	Transport Transport // how to reach peers

	// Storage persists term, vote, log and snapshot. Default: a new
	// MemStorage, which is not durable across process restarts. The caller
	// owns it: Stop does not close it.
	Storage Storage
	// Apply receives each committed, non-empty entry in log order. It is
	// called from one goroutine at a time, without any Node lock held, and
	// must be deterministic. Stop waits for a running Apply call to return.
	Apply func(Entry)

	// Snapshot serializes the state machine. It is called from the same
	// goroutine as Apply, between entries, so the state is exactly "everything
	// applied so far". Required when SnapshotThreshold > 0.
	Snapshot func() ([]byte, error)
	// Restore replaces the state machine's entire state with a snapshot
	// produced by Snapshot (possibly on another node). It is called from the
	// Apply goroutine. A returned error takes the node out of service.
	Restore func(data []byte) error
	// SnapshotThreshold is the number of applied entries after the last
	// snapshot that triggers a new one. 0 disables snapshotting on this node
	// (it can still receive and restore snapshots).
	SnapshotThreshold uint64

	// ElectionTimeoutMin/Max bound the randomized election timeout.
	// Defaults: 150ms and 300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval must be well below ElectionTimeoutMin. Default: 50ms.
	HeartbeatInterval time.Duration

	// Seed seeds the timeout randomizer. 0 means "use the current time".
	Seed int64
	// Logger is optional; by default nothing is logged.
	Logger *slog.Logger
}

func (c *Config) fillAndValidate() error {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}

	switch {
	case c.ID == "":
		return fmt.Errorf("%w: empty ID", ErrInvalidConfig)
	case c.Transport == nil:
		return fmt.Errorf("%w: nil Transport", ErrInvalidConfig)
	case c.ElectionTimeoutMin < 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin:
		return fmt.Errorf("%w: need 0 < ElectionTimeoutMin <= ElectionTimeoutMax", ErrInvalidConfig)
	case c.HeartbeatInterval < 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin:
		return fmt.Errorf("%w: HeartbeatInterval must be positive and below ElectionTimeoutMin", ErrInvalidConfig)
	case c.SnapshotThreshold > 0 && c.Snapshot == nil:
		return fmt.Errorf("%w: SnapshotThreshold needs a Snapshot function", ErrInvalidConfig)
	}

	seen := map[string]bool{c.ID: true}
	for _, p := range c.Peers {
		if p == "" {
			return fmt.Errorf("%w: empty peer ID", ErrInvalidConfig)
		}
		if seen[p] {
			return fmt.Errorf("%w: peer %q is a duplicate or this node itself", ErrInvalidConfig, p)
		}
		seen[p] = true
	}
	return nil
}

// Status is a consistent snapshot of a node's externally visible state.
type Status struct {
	ID            string
	State         State
	Term          uint64
	LeaderID      string // who this node believes is leader in Term ("" if unknown)
	LastIndex     uint64 // index of the last log entry
	CommitIndex   uint64 // highest index known to be committed
	Applied       uint64 // highest index handed to Apply (or covered by a restored snapshot)
	SnapshotIndex uint64 // last index covered by the snapshot (0 = none): the log before it is compacted
	Err           error  // non-nil once a storage failure has taken the node out of service
}

// peerProgress is the leader's view of one follower's log.
type peerProgress struct {
	next     uint64 // index of the next entry to send
	match    uint64 // highest index known to be replicated on the follower
	inflight bool   // an AppendEntries or InstallSnapshot RPC is outstanding
	ackSeq   uint64 // newest read sequence number this follower has acknowledged leadership for
}

// Node is one Raft participant. All methods are safe for concurrent use.
type Node struct {
	id            string
	peers         []string
	tr            Transport
	storage       Storage
	apply         func(Entry)
	snapshotFn    func() ([]byte, error)
	restoreFn     func([]byte) error
	snapThreshold uint64
	electionMin   time.Duration
	electionMax   time.Duration
	heartbeat     time.Duration
	logger        *slog.Logger

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	applyCh chan struct{} // wakes the applier; buffered so signalling never blocks

	mu    sync.Mutex // guards everything below
	rng   *rand.Rand
	state State
	term  uint64
	// log[0] is a sentinel standing for the snapshot: its Index and Term are
	// the snapshot's last included entry (0, 0 if there is none). Entry i
	// lives at log[i-log[0].Index].
	log              []Entry
	snapshot         Snapshot  // the snapshot log[0] stands for (kept to send to lagging followers)
	pendingSnap      *Snapshot // a snapshot the apply goroutine must still restore
	votedFor         string
	commitIndex      uint64
	lastApplied      uint64
	leaderID         string
	progress         map[string]*peerProgress // leader only
	termStart        uint64                   // leader only: index of the no-op appended when this term began
	readSeq          uint64                   // leader only: number of ReadIndex requests so far
	reads            []*readRequest           // leader only: reads waiting for a quorum confirmation
	electionDeadline time.Time
	nextHeartbeat    time.Time
	started          bool
	stopped          bool
	failed           error // sticky storage failure; the node is out of service
}

// NewNode validates cfg, loads any persisted state and creates a stopped
// node. Call Start to run it.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.fillAndValidate(); err != nil {
		return nil, err
	}
	st := cfg.Storage
	if st == nil {
		st = NewMemStorage()
	}
	hs, entries, err := st.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	snap, err := st.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	lg := make([]Entry, 1, len(entries)+1)
	lg[0] = Entry{Index: snap.Index, Term: snap.Term}
	for i, e := range entries {
		if want := snap.Index + uint64(i) + 1; e.Index != want {
			return nil, fmt.Errorf("%w: log entry %d has index %d", ErrCorruptStorage, want, e.Index)
		}
		lg = append(lg, e)
	}

	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(discardHandler{})
	}
	apply := cfg.Apply
	if apply == nil {
		apply = func(Entry) {}
	}
	restore := cfg.Restore
	if restore == nil {
		restore = func([]byte) error { return nil }
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:            cfg.ID,
		peers:         append([]string(nil), cfg.Peers...),
		tr:            cfg.Transport,
		storage:       st,
		apply:         apply,
		snapshotFn:    cfg.Snapshot,
		restoreFn:     restore,
		snapThreshold: cfg.SnapshotThreshold,
		electionMin:   cfg.ElectionTimeoutMin,
		electionMax:   cfg.ElectionTimeoutMax,
		heartbeat:     cfg.HeartbeatInterval,
		logger:        logger.With("node", cfg.ID),
		ctx:           ctx,
		cancel:        cancel,
		applyCh:       make(chan struct{}, 1),
		rng:           rand.New(rand.NewSource(seed)),
		term:          hs.Term,
		votedFor:      hs.VotedFor,
		log:           lg,
		snapshot:      snap,
	}
	if snap.Index > 0 {
		// Everything up to the snapshot is committed. The state machine
		// still has to be restored from it before entries after it apply.
		n.commitIndex = snap.Index
		ps := snap
		n.pendingSnap = &ps
	}
	n.resetElectionTimer()
	return n, nil
}

// Start launches the node's background goroutines. It does nothing if the
// node was already started or stopped.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopped {
		return
	}
	n.started = true
	n.resetElectionTimer()
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
	if n.pendingSnap != nil {
		n.signalApply()
	}
}

// Stop halts the node and waits for its goroutines. It is idempotent. A
// stopped node answers RPCs with a rejection and never changes state.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.mu.Unlock()

	n.cancel()
	n.wg.Wait()
}

// Status returns the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:            n.id,
		State:         n.state,
		Term:          n.term,
		LeaderID:      n.leaderID,
		LastIndex:     n.lastIndex(),
		CommitIndex:   n.commitIndex,
		Applied:       n.lastApplied,
		SnapshotIndex: n.snapIndex(),
		Err:           n.failed,
	}
}

// Propose appends data to the log if this node is the leader and starts
// replicating it. It returns as soon as the entry is durable on the leader:
// it does NOT wait for the entry to commit. The returned index and term
// identify the entry; the write is committed once an entry with that index
// AND term is applied. If a different term shows up at that index, the
// entry was overwritten by a newer leader and the write did not happen.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if len(data) == 0 {
		return 0, 0, ErrEmptyCommand
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	switch {
	case n.stopped:
		return 0, 0, ErrStopped
	case n.failed != nil:
		return 0, 0, n.failed
	case n.state != Leader:
		return 0, 0, ErrNotLeader
	}

	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: append([]byte(nil), data...)}
	if err := n.appendLocal(e); err != nil {
		return 0, 0, err
	}
	n.advanceCommit() // commits immediately in a single-node cluster
	n.broadcast(time.Now())
	return e.Index, e.Term, nil
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.down() || args.Term < n.term {
		return &RequestVoteReply{Term: n.term}
	}
	if args.Term > n.term {
		n.becomeFollower(args.Term)
		if n.failed != nil {
			return &RequestVoteReply{Term: n.term}
		}
	}

	// Election restriction (Raft section 5.4.1): only vote for candidates
	// whose log is at least as up to date as ours.
	lastIndex, lastTerm := n.lastLog()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		if n.votedFor == "" {
			n.votedFor = args.CandidateID
			// The vote must be durable before the candidate can count it.
			if !n.persistHardState() {
				return &RequestVoteReply{Term: n.term}
			}
		}
		n.resetElectionTimer() // we just supported a candidate: give it time to win
		n.logger.Debug("vote granted", "term", n.term, "candidate", args.CandidateID)
		return &RequestVoteReply{Term: n.term, VoteGranted: true}
	}
	return &RequestVoteReply{Term: n.term}
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.down() || args.Term < n.term {
		return &AppendEntriesReply{Term: n.term}
	}
	// A valid leader exists for args.Term. A candidate in the same term
	// must also step down.
	if args.Term > n.term || n.state != Follower {
		n.becomeFollower(args.Term)
		if n.failed != nil {
			return &AppendEntriesReply{Term: n.term}
		}
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	prevIdx, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries

	// The RPC may start before our snapshot. Everything the snapshot covers
	// is committed and identical on the leader, so skip those entries.
	if si := n.snapIndex(); prevIdx < si {
		drop := si - prevIdx
		if drop > uint64(len(entries)) {
			drop = uint64(len(entries))
		}
		if drop > 0 {
			prevIdx += drop
			prevTerm = entries[drop-1].Term
			entries = entries[drop:]
		}
		if prevIdx < si {
			// The RPC ends inside our snapshot: all of it is already here.
			return &AppendEntriesReply{Term: n.term, Success: true}
		}
	}

	// Consistency check: our log must contain the entry the new ones follow.
	last := n.lastIndex()
	if prevIdx > last {
		return &AppendEntriesReply{Term: n.term, ConflictIndex: last + 1}
	}
	if t := n.termAt(prevIdx); t != prevTerm {
		ci := prevIdx
		for ci > 1 && n.termAt(ci-1) == t {
			ci--
		}
		return &AppendEntriesReply{Term: n.term, ConflictIndex: ci}
	}

	if !n.storeEntries(prevIdx, entries) {
		return &AppendEntriesReply{Term: n.term}
	}

	// Advance the commit index, but only as far as the entries this RPC
	// proved to match the leader: our log may hold extra, unverified entries.
	if args.LeaderCommit > n.commitIndex {
		lastNew := prevIdx + uint64(len(entries))
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.signalApply()
		}
	}
	return &AppendEntriesReply{Term: n.term, Success: true}
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.down() || args.Term < n.term {
		return &InstallSnapshotReply{Term: n.term}
	}
	if args.Term > n.term || n.state != Follower {
		n.becomeFollower(args.Term)
		if n.failed != nil {
			return &InstallSnapshotReply{Term: n.term}
		}
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	idx, term := args.LastIncludedIndex, args.LastIncludedTerm
	if idx == 0 {
		return &InstallSnapshotReply{Term: n.term}
	}
	if idx <= n.commitIndex {
		// We already hold (and have committed) everything this covers.
		return &InstallSnapshotReply{Term: n.term, Success: true}
	}

	snap := Snapshot{Index: idx, Term: term, Data: append([]byte(nil), args.Data...)}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		n.fail(fmt.Errorf("persist snapshot: %w", err))
		return &InstallSnapshotReply{Term: n.term}
	}

	// Mirror the storage rule: keep the entries after idx only if our log
	// agrees with the snapshot at idx.
	si := n.snapIndex()
	if idx <= n.lastIndex() && n.termAt(idx) == term {
		n.log = append([]Entry{{Index: idx, Term: term}}, n.log[idx-si+1:]...)
	} else {
		n.log = []Entry{{Index: idx, Term: term}}
	}
	n.snapshot = snap
	n.commitIndex = idx
	n.pendingSnap = &snap
	n.signalApply()
	n.logger.Info("installed snapshot", "index", idx, "term", term)
	return &InstallSnapshotReply{Term: n.term, Success: true}
}

// ---- internals (n.mu must be held unless stated otherwise) ----------------

func (n *Node) run() {
	defer n.wg.Done()

	tick := n.heartbeat / 2
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down() {
		return
	}
	now := time.Now()
	if n.state == Leader {
		// Pending reads need a fresh round of acknowledgements, so they
		// trigger a broadcast without waiting for the heartbeat deadline.
		if len(n.reads) > 0 || !now.Before(n.nextHeartbeat) {
			n.broadcast(now)
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElection()
	}
}

// applyLoop restores snapshots and hands committed entries to Apply, outside
// the lock, in order. It is the only goroutine that touches the state machine.
func (n *Node) applyLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applyCh:
		}
		for n.applyPending() {
		}
	}
}

// applyPending does one unit of apply work and reports whether more may remain.
func (n *Node) applyPending() bool {
	n.mu.Lock()
	if n.down() {
		n.mu.Unlock()
		return false
	}

	if snap := n.pendingSnap; snap != nil {
		n.pendingSnap = nil
		n.mu.Unlock()

		if err := n.restoreFn(snap.Data); err != nil {
			n.mu.Lock()
			n.fail(fmt.Errorf("restore snapshot: %w", err))
			n.mu.Unlock()
			return false
		}
		n.mu.Lock()
		if snap.Index > n.lastApplied {
			n.lastApplied = snap.Index
		}
		n.mu.Unlock()
		return true
	}

	if n.lastApplied >= n.commitIndex {
		n.mu.Unlock()
		return false
	}
	si := n.snapIndex()
	from, to := n.lastApplied+1, n.commitIndex
	if from <= si {
		n.fail(fmt.Errorf("%w: entry %d needed for apply is compacted (snapshot at %d)", ErrCorruptStorage, from, si))
		n.mu.Unlock()
		return false
	}
	batch := append([]Entry(nil), n.log[from-si:to-si+1]...)
	n.mu.Unlock()

	for _, e := range batch {
		if len(e.Data) > 0 { // skip leader no-ops
			n.apply(e)
		}
	}

	n.mu.Lock()
	if to > n.lastApplied {
		n.lastApplied = to
	}
	n.mu.Unlock()

	n.maybeSnapshot()
	return true
}

// maybeSnapshot takes a snapshot if enough entries were applied since the
// last one. It runs on the apply goroutine, so the state machine is exactly
// at lastApplied while it is serialized.
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	if n.down() || n.snapThreshold == 0 || n.snapshotFn == nil || n.pendingSnap != nil ||
		n.lastApplied <= n.snapIndex() || n.lastApplied-n.snapIndex() < n.snapThreshold {
		n.mu.Unlock()
		return
	}
	index := n.lastApplied
	n.mu.Unlock()

	data, err := n.snapshotFn()
	if err != nil {
		n.logger.Error("snapshot failed; will retry", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// A snapshot from the leader may have replaced our state meanwhile.
	if n.down() || n.pendingSnap != nil || index <= n.snapIndex() || index > n.commitIndex {
		return
	}
	n.compact(index, data)
}

// compact makes data the snapshot at index and drops the log before it.
func (n *Node) compact(index uint64, data []byte) {
	si := n.snapIndex()
	term := n.log[index-si].Term
	snap := Snapshot{Index: index, Term: term, Data: data}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		n.fail(fmt.Errorf("persist snapshot: %w", err))
		return
	}
	n.log = append([]Entry{{Index: index, Term: term}}, n.log[index-si+1:]...)
	n.snapshot = snap
	n.logger.Info("log compacted", "snapshot_index", index, "log_entries", len(n.log)-1)
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// down reports whether the node must not take part in the cluster.
func (n *Node) down() bool { return n.stopped || n.failed != nil }

// fail takes the node out of service after a storage error. Continuing would
// risk acknowledging state that is not durable.
func (n *Node) fail(err error) {
	if n.failed == nil {
		n.failed = err
		n.logger.Error("node failed; out of service", "err", err)
		n.failReads(err)
	}
}

func (n *Node) persistHardState() bool {
	hs := HardState{Term: n.term, VotedFor: n.votedFor}
	if err := n.storage.SaveHardState(hs); err != nil {
		n.fail(fmt.Errorf("persist hard state: %w", err))
		return false
	}
	return true
}

// appendLocal durably appends entries, then adds them to the in-memory log.
func (n *Node) appendLocal(entries ...Entry) error {
	if err := n.storage.Append(entries); err != nil {
		n.fail(fmt.Errorf("persist log: %w", err))
		return n.failed
	}
	n.log = append(n.log, entries...)
	return nil
}

// snapIndex is the last index covered by the snapshot (0 if none).
func (n *Node) snapIndex() uint64 { return n.log[0].Index }

func (n *Node) lastIndex() uint64 { return n.log[0].Index + uint64(len(n.log)-1) }

func (n *Node) lastLog() (index, term uint64) {
	return n.lastIndex(), n.log[len(n.log)-1].Term
}

// termAt returns the term of the entry at index i. It returns 0 for an index
// that is not in the log: beyond its end, or compacted into a snapshot (the
// snapshot's own last index is still known).
func (n *Node) termAt(i uint64) uint64 {
	si := n.snapIndex()
	if i < si || i > n.lastIndex() {
		return 0
	}
	return n.log[i-si].Term
}

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// resetElectionTimer picks a fresh random deadline in [min, max).
func (n *Node) resetElectionTimer() {
	d := n.electionMin
	if span := n.electionMax - n.electionMin; span > 0 {
		d += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = time.Now().Add(d)
}

// becomeFollower moves to Follower. A higher term clears our vote and leader
// and is persisted. Reads still waiting for confirmation fail: we can no
// longer vouch for our leadership.
func (n *Node) becomeFollower(term uint64) {
	if n.state != Follower {
		n.logger.Info("stepping down", "from", n.state.String(), "term", term)
	}
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.leaderID = ""
		n.persistHardState()
	}
	if n.state == Leader {
		n.resetElectionTimer() // a leader's old deadline is meaningless
	}
	n.failReads(ErrNotLeader)
	n.progress = nil
	n.state = Follower
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.term)

	last := n.lastIndex()
	n.progress = make(map[string]*peerProgress, len(n.peers))
	for _, peer := range n.peers {
		n.progress[peer] = &peerProgress{next: last + 1}
	}
	n.termStart = last + 1
	n.readSeq = 0
	n.reads = nil

	// A no-op in the new term: once it commits, every earlier entry commits
	// with it (Raft paper section 5.4.2 / figure 8).
	if err := n.appendLocal(Entry{Index: last + 1, Term: n.term}); err != nil {
		return
	}
	n.advanceCommit()
	n.broadcast(time.Now())
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.progress = nil
	n.resetElectionTimer() // if this election splits, we retry after a new random delay
	if !n.persistHardState() {
		return
	}
	term := n.term
	n.logger.Debug("starting election", "term", term)

	if len(n.peers) == 0 { // single-node cluster
		n.becomeLeader()
		return
	}

	votes := 1 // our own; guarded by n.mu
	lastIndex, lastTerm := n.lastLog()
	args := &RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}

	for _, peer := range n.peers {
		n.spawn(func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.electionMin)
			defer cancel()
			reply, err := n.tr.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if n.down() {
				return
			}
			if reply.Term > n.term {
				n.becomeFollower(reply.Term)
				return
			}
			// Ignore replies for an election that is no longer current.
			if n.state != Candidate || n.term != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= n.quorum() {
				n.becomeLeader()
			}
		})
	}
}

// broadcast sends each follower whatever it is missing (or a heartbeat).
func (n *Node) broadcast(now time.Time) {
	n.nextHeartbeat = now.Add(n.heartbeat)
	for _, peer := range n.peers {
		n.replicate(peer)
	}
}

// replicate sends one AppendEntries (or, if the follower needs entries that
// were compacted away, one InstallSnapshot) to peer unless an RPC is already
// in flight. When a reply shows more work to do, the reply handler calls it again.
func (n *Node) replicate(peer string) {
	p := n.progress[peer]
	if p == nil || p.inflight || n.state != Leader || n.down() {
		return
	}

	si := n.snapIndex()
	if p.next <= si {
		n.sendSnapshot(peer, p)
		return
	}

	prev := p.next - 1
	last := n.lastIndex()
	var entries []Entry
	if p.next <= last {
		end := last + 1
		if end-p.next > maxEntriesPerRPC {
			end = p.next + maxEntriesPerRPC
		}
		entries = append([]Entry(nil), n.log[p.next-si:end-si]...)
	}
	args := &AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.term
	// Any read requested after this point has a higher sequence number, so an
	// acknowledgement of this RPC can never confirm it.
	seq := n.readSeq
	p.inflight = true

	n.spawn(func() {
		ctx, cancel := context.WithTimeout(n.ctx, n.electionMin)
		defer cancel()
		reply, err := n.tr.AppendEntries(ctx, peer, args)

		n.mu.Lock()
		defer n.mu.Unlock()
		p.inflight = false
		if err != nil || n.down() {
			return
		}
		n.handleAppendReply(peer, p, term, seq, args, reply)
	})
}

func (n *Node) sendSnapshot(peer string, p *peerProgress) {
	snap := n.snapshot
	args := &InstallSnapshotArgs{
		Term:              n.term,
		LeaderID:          n.id,
		LastIncludedIndex: snap.Index,
		LastIncludedTerm:  snap.Term,
		Data:              snap.Data, // immutable: shared, never modified
	}
	term := n.term
	seq := n.readSeq
	p.inflight = true

	n.spawn(func() {
		ctx, cancel := context.WithTimeout(n.ctx, snapshotRPCTimeout)
		defer cancel()
		reply, err := n.tr.InstallSnapshot(ctx, peer, args)

		n.mu.Lock()
		defer n.mu.Unlock()
		p.inflight = false
		if err != nil || n.down() {
			return
		}
		n.handleSnapshotReply(peer, p, term, seq, args, reply)
	})
}

func (n *Node) handleSnapshotReply(peer string, p *peerProgress, term, seq uint64, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		return
	}
	if n.state != Leader || n.term != term || n.progress[peer] != p {
		return
	}
	if !reply.Success {
		return // retried on the next heartbeat
	}
	// The follower accepted us as leader of this term.
	if seq > p.ackSeq {
		p.ackSeq = seq
	}
	n.completeReads()
	n.matchAdvanced(peer, p, args.LastIncludedIndex)
}

func (n *Node) handleAppendReply(peer string, p *peerProgress, term, seq uint64, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		return
	}
	// Ignore replies that belong to an earlier leadership of ours.
	if n.state != Leader || n.term != term || n.progress[peer] != p {
		return
	}

	// The follower accepted us as leader of this term (it got past the term
	// check, whether or not the logs matched), so it confirms any read that
	// was requested before this RPC was sent.
	if reply.Success || reply.ConflictIndex > 0 {
		if seq > p.ackSeq {
			p.ackSeq = seq
		}
		n.completeReads()
	}

	if reply.Success {
		n.matchAdvanced(peer, p, args.PrevLogIndex+uint64(len(args.Entries)))
		return
	}

	// Rejected. Only a genuine log mismatch (ConflictIndex > 0) is retried
	// immediately; anything else waits for the next heartbeat.
	if reply.ConflictIndex == 0 || p.next <= 1 || p.next != args.PrevLogIndex+1 {
		return
	}
	next := reply.ConflictIndex
	if next >= p.next {
		next = p.next - 1 // always move backwards so the search terminates
	}
	if next < 1 {
		next = 1
	}
	p.next = next
	n.replicate(peer) // sends a snapshot instead if next is now inside the compacted prefix
}

// matchAdvanced records that peer holds everything up to match, commits what
// that allows, and keeps sending until the peer has caught up.
func (n *Node) matchAdvanced(peer string, p *peerProgress, match uint64) {
	if match > p.match {
		p.match = match
	}
	if p.next < match+1 {
		p.next = match + 1
	}
	if n.advanceCommit() {
		// Tell followers the new commit index without waiting for the
		// next heartbeat.
		for _, other := range n.peers {
			n.replicate(other)
		}
	}
	if p.next <= n.lastIndex() {
		n.replicate(peer)
	}
}

// advanceCommit moves the commit index to the highest entry of the CURRENT
// term stored on a majority. It reports whether the index moved.
func (n *Node) advanceCommit() bool {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.term {
			return false // terms never decrease along the log: nothing below qualifies either
		}
		count := 1 // the leader itself
		for _, peer := range n.peers {
			if p := n.progress[peer]; p != nil && p.match >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.signalApply()
			return true
		}
	}
	return false
}

// storeEntries applies the log-update part of AppendEntries: skip entries we
// already have, truncate on conflict, append the rest. prevIdx is the index
// the entries follow (never below our snapshot). It reports success.
func (n *Node) storeEntries(prevIdx uint64, entries []Entry) bool {
	last := n.lastIndex()
	si := n.snapIndex()
	from := -1
	var truncateAt uint64
	for i := range entries {
		idx := prevIdx + 1 + uint64(i)
		if idx > last {
			from = i
			break
		}
		if n.log[idx-si].Term != entries[i].Term {
			truncateAt, from = idx, i
			break
		}
	}
	if from < 0 {
		return true // nothing new (e.g. a heartbeat or a duplicate)
	}

	if truncateAt != 0 {
		if truncateAt <= n.commitIndex {
			// A correct leader never asks for this; refuse instead of
			// destroying committed data.
			n.logger.Error("refusing to overwrite committed entries",
				"index", truncateAt, "commit", n.commitIndex)
			return false
		}
		if err := n.storage.TruncateFrom(truncateAt); err != nil {
			n.fail(fmt.Errorf("persist log truncation: %w", err))
			return false
		}
		n.log = n.log[:truncateAt-si]
	}

	fresh := make([]Entry, 0, len(entries)-from)
	for i := from; i < len(entries); i++ {
		fresh = append(fresh, Entry{
			Index: prevIdx + 1 + uint64(i),
			Term:  entries[i].Term,
			Data:  entries[i].Data,
		})
	}
	return n.appendLocal(fresh...) == nil
}

// spawn runs f in a tracked goroutine. n.mu must be held; holding it makes
// the stopped check and wg.Add atomic with respect to Stop.
func (n *Node) spawn(f func()) {
	if n.stopped {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

// discardHandler is a slog.Handler that drops everything.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
