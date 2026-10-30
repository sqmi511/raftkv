// Package raft implements the Raft consensus algorithm.
//
// This file covers leader election: node states, terms, randomized election
// timeouts, RequestVote and heartbeats. Log replication and persistence are
// added in the next task.
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

// ErrInvalidConfig is wrapped by errors returned from NewNode.
var ErrInvalidConfig = errors.New("raft: invalid config")

// Config configures a Node.
type Config struct {
	ID        string    // this node's ID; must be unique in the cluster
	Peers     []string  // IDs of all OTHER nodes
	Transport Transport // how to reach peers

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
	ID       string
	State    State
	Term     uint64
	LeaderID string // who this node believes is leader in Term ("" if unknown)
}

// Node is one Raft participant. All methods are safe for concurrent use.
type Node struct {
	id          string
	peers       []string
	tr          Transport
	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	logger      *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu               sync.Mutex // guards everything below
	rng              *rand.Rand
	state            State
	term             uint64
	votedFor         string
	leaderID         string
	electionDeadline time.Time
	nextHeartbeat    time.Time
	started          bool
	stopped          bool
}

// NewNode validates cfg and creates a stopped node. Call Start to run it.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.fillAndValidate(); err != nil {
		return nil, err
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(discardHandler{})
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:          cfg.ID,
		peers:       append([]string(nil), cfg.Peers...),
		tr:          cfg.Transport,
		electionMin: cfg.ElectionTimeoutMin,
		electionMax: cfg.ElectionTimeoutMax,
		heartbeat:   cfg.HeartbeatInterval,
		logger:      logger.With("node", cfg.ID),
		ctx:         ctx,
		cancel:      cancel,
		rng:         rand.New(rand.NewSource(seed)),
	}
	n.resetElectionTimer()
	return n, nil
}

// Start launches the node's background loop. It does nothing if the node was
// already started or stopped.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopped {
		return
	}
	n.started = true
	n.resetElectionTimer()
	n.wg.Add(1)
	go n.run()
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

// Status returns the node's current state, term and known leader.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, State: n.state, Term: n.term, LeaderID: n.leaderID}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped || args.Term < n.term {
		return &RequestVoteReply{Term: n.term}
	}
	if args.Term > n.term {
		n.becomeFollower(args.Term)
	}

	// Election restriction (Raft section 5.4.1): only vote for candidates
	// whose log is at least as up to date as ours.
	lastIndex, lastTerm := n.lastLog()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.resetElectionTimer() // we just supported a candidate: give it time to win
		n.logger.Debug("vote granted", "term", n.term, "candidate", args.CandidateID)
		return &RequestVoteReply{Term: n.term, VoteGranted: true}
	}
	return &RequestVoteReply{Term: n.term}
}

// HandleAppendEntries processes an incoming AppendEntries RPC (heartbeat).
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped || args.Term < n.term {
		return &AppendEntriesReply{Term: n.term}
	}
	// A valid leader exists for args.Term. A candidate in the same term
	// must also step down.
	if args.Term > n.term || n.state != Follower {
		n.becomeFollower(args.Term)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
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
	if n.stopped {
		return
	}
	now := time.Now()
	if n.state == Leader {
		if !now.Before(n.nextHeartbeat) {
			n.sendHeartbeats(now)
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElection()
	}
}

// lastLog returns the index and term of our last log entry. There is no log
// yet (added in the next task), so this is always the empty log.
func (n *Node) lastLog() (index, term uint64) { return 0, 0 }

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// resetElectionTimer picks a fresh random deadline in [min, max).
func (n *Node) resetElectionTimer() {
	d := n.electionMin
	if span := n.electionMax - n.electionMin; span > 0 {
		d += time.Duration(n.rng.Int63n(int64(span)))
	}
	n.electionDeadline = time.Now().Add(d)
}

// becomeFollower moves to Follower. A higher term clears our vote and leader.
func (n *Node) becomeFollower(term uint64) {
	if n.state != Follower {
		n.logger.Info("stepping down", "from", n.state.String(), "term", term)
	}
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.leaderID = ""
	}
	if n.state == Leader {
		n.resetElectionTimer() // a leader's old deadline is meaningless
	}
	n.state = Follower
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.logger.Info("became leader", "term", n.term)
	n.sendHeartbeats(time.Now())
}

func (n *Node) startElection() {
	n.state = Candidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionTimer() // if this election splits, we retry after a new random delay
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
			if n.stopped {
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

func (n *Node) sendHeartbeats(now time.Time) {
	n.nextHeartbeat = now.Add(n.heartbeat)
	term := n.term
	args := &AppendEntriesArgs{Term: term, LeaderID: n.id}

	for _, peer := range n.peers {
		n.spawn(func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.electionMin)
			defer cancel()
			reply, err := n.tr.AppendEntries(ctx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.term {
				n.becomeFollower(reply.Term)
			}
		})
	}
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
