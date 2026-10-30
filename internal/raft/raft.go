// Package raft implements the Raft consensus algorithm: leader election, log
// replication, commit tracking and persistence of term, vote and log.
//
// A Node applies committed entries, in log order, from a single goroutine by
// calling Config.Apply. Without snapshots the state machine is rebuilt from
// the persisted log after a restart, so Apply starts again from index 1 and
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
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrEmptyCommand is returned by Propose for an empty command.
	ErrEmptyCommand = errors.New("raft: empty command")
	// ErrStopped is returned by Propose after Stop.
	ErrStopped = errors.New("raft: node stopped")
)

// maxEntriesPerRPC bounds how many log entries one AppendEntries carries.
const maxEntriesPerRPC = 256

// Config configures a Node.
type Config struct {
	ID        string    // this node's ID; must be unique in the cluster
	Peers     []string  // IDs of all OTHER nodes
	Transport Transport // how to reach peers

	// Storage persists term, vote and log. Default: a new MemStorage, which
	// is not durable across process restarts. The caller owns it: Stop does
	// not close it.
	Storage Storage
	// Apply receives each committed, non-empty entry in log order. It is
	// called from one goroutine at a time, without any Node lock held, and
	// must be deterministic. Stop waits for a running Apply call to return.
	Apply func(Entry)

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
	ID          string
	State       State
	Term        uint64
	LeaderID    string // who this node believes is leader in Term ("" if unknown)
	LastIndex   uint64 // index of the last log entry
	CommitIndex uint64 // highest index known to be committed
	Applied     uint64 // highest index handed to Apply
	Err         error  // non-nil once a storage failure has taken the node out of service
}

// peerProgress is the leader's view of one follower's log.
type peerProgress struct {
	next     uint64 // index of the next entry to send
	match    uint64 // highest index known to be replicated on the follower
	inflight bool   // an AppendEntries RPC is outstanding
}

// Node is one Raft participant. All methods are safe for concurrent use.
type Node struct {
	id          string
	peers       []string
	tr          Transport
	storage     Storage
	apply       func(Entry)
	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	logger      *slog.Logger

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	applyCh chan struct{} // wakes the applier; buffered so signalling never blocks

	mu               sync.Mutex // guards everything below
	rng              *rand.Rand
	state            State
	term             uint64
	votedFor         string
	log              []Entry // log[0] is a sentinel; entry i lives at log[i]
	commitIndex      uint64
	lastApplied      uint64
	leaderID         string
	progress         map[string]*peerProgress // leader only
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
	lg := make([]Entry, 1, len(entries)+1)
	for i, e := range entries {
		if e.Index != uint64(i)+1 {
			return nil, fmt.Errorf("%w: log entry %d has index %d", ErrCorruptStorage, i+1, e.Index)
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

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:          cfg.ID,
		peers:       append([]string(nil), cfg.Peers...),
		tr:          cfg.Transport,
		storage:     st,
		apply:       apply,
		electionMin: cfg.ElectionTimeoutMin,
		electionMax: cfg.ElectionTimeoutMax,
		heartbeat:   cfg.HeartbeatInterval,
		logger:      logger.With("node", cfg.ID),
		ctx:         ctx,
		cancel:      cancel,
		applyCh:     make(chan struct{}, 1),
		rng:         rand.New(rand.NewSource(seed)),
		term:        hs.Term,
		votedFor:    hs.VotedFor,
		log:         lg,
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
		ID:          n.id,
		State:       n.state,
		Term:        n.term,
		LeaderID:    n.leaderID,
		LastIndex:   n.lastIndex(),
		CommitIndex: n.commitIndex,
		Applied:     n.lastApplied,
		Err:         n.failed,
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

	// Consistency check: our log must contain the entry the new ones follow.
	last := n.lastIndex()
	if args.PrevLogIndex > last {
		return &AppendEntriesReply{Term: n.term, ConflictIndex: last + 1}
	}
	if t := n.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		ci := args.PrevLogIndex
		for ci > 1 && n.termAt(ci-1) == t {
			ci--
		}
		return &AppendEntriesReply{Term: n.term, ConflictIndex: ci}
	}

	if !n.storeEntries(args) {
		return &AppendEntriesReply{Term: n.term}
	}

	// Advance the commit index, but only as far as the entries this RPC
	// proved to match the leader: our log may hold extra, unverified entries.
	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
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
		if !now.Before(n.nextHeartbeat) {
			n.broadcast(now)
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElection()
	}
}

// applyLoop hands committed entries to Apply, outside the lock, in order.
func (n *Node) applyLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applyCh:
		}
		for {
			n.mu.Lock()
			if n.down() || n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			from, to := n.lastApplied+1, n.commitIndex
			batch := append([]Entry(nil), n.log[from:to+1]...)
			n.mu.Unlock()

			for _, e := range batch {
				if len(e.Data) > 0 { // skip leader no-ops
					n.apply(e)
				}
			}

			n.mu.Lock()
			n.lastApplied = to
			n.mu.Unlock()
		}
	}
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

func (n *Node) lastIndex() uint64 { return uint64(len(n.log) - 1) }

func (n *Node) lastLog() (index, term uint64) {
	index = n.lastIndex()
	return index, n.log[index].Term
}

// termAt returns the term of the entry at index i (0 for the sentinel or an
// index beyond the log).
func (n *Node) termAt(i uint64) uint64 {
	if i >= uint64(len(n.log)) {
		return 0
	}
	return n.log[i].Term
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
// and is persisted.
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

// replicate sends one AppendEntries to peer unless one is already in flight.
// When a reply shows more work to do, the reply handler calls it again.
func (n *Node) replicate(peer string) {
	p := n.progress[peer]
	if p == nil || p.inflight || n.state != Leader || n.down() {
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
		entries = append([]Entry(nil), n.log[p.next:end]...)
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
		n.handleAppendReply(peer, p, term, args, reply)
	})
}

func (n *Node) handleAppendReply(peer string, p *peerProgress, term uint64, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > n.term {
		n.becomeFollower(reply.Term)
		return
	}
	// Ignore replies that belong to an earlier leadership of ours.
	if n.state != Leader || n.term != term || n.progress[peer] != p {
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
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
			n.replicate(peer) // keep sending until the follower has caught up
		}
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
	n.replicate(peer)
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
// already have, truncate on conflict, append the rest. It reports success.
func (n *Node) storeEntries(args *AppendEntriesArgs) bool {
	last := n.lastIndex()
	from := -1
	var truncateAt uint64
	for i := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx > last {
			from = i
			break
		}
		if n.log[idx].Term != args.Entries[i].Term {
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
		n.log = n.log[:truncateAt]
	}

	fresh := make([]Entry, 0, len(args.Entries)-from)
	for i := from; i < len(args.Entries); i++ {
		fresh = append(fresh, Entry{
			Index: args.PrevLogIndex + 1 + uint64(i),
			Term:  args.Entries[i].Term,
			Data:  args.Entries[i].Data,
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
