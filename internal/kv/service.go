package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/store"
)

const (
	leaderCheckInterval = 20 * time.Millisecond // how often a waiting write re-checks that we still lead
	notReadyRetry       = 2 * time.Millisecond  // pause between ReadIndex attempts right after an election
	applyPoll           = time.Millisecond      // how often a read re-checks that the state machine caught up
)

// DefaultSnapshotThreshold is the number of applied entries after which a
// node snapshots its state and compacts its log.
const DefaultSnapshotThreshold = 10_000

var (
	// ErrInvalidRequest is returned for a write without a client ID or with request ID 0.
	ErrInvalidRequest = errors.New("kv: invalid request")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("kv: service stopped")
	// ErrLeadershipLost means this node stopped being leader while the write
	// was in flight. The write may still commit under the new leader, so the
	// caller must retry with the SAME request ID.
	ErrLeadershipLost = errors.New("kv: leadership lost before the write was confirmed")
	// ErrOverwritten means another leader replaced the entry: the write did
	// not happen at that log position. Retry with the same request ID.
	ErrOverwritten = errors.New("kv: write was overwritten by a newer leader")
	// ErrStaleRequest means the request ID is older than the client's latest.
	ErrStaleRequest = errors.New("kv: request id is older than the client's latest request")
)

// NotLeaderError is returned by a node that cannot serve the request because
// it is not the leader. LeaderID is who it believes the leader is ("" if unknown).
type NotLeaderError struct {
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "kv: not the leader (leader unknown)"
	}
	return "kv: not the leader (leader is " + e.LeaderID + ")"
}

// Config configures a Service.
type Config struct {
	// Raft configures the underlying consensus node. Apply, Snapshot,
	// Restore and SnapshotThreshold must be unset: the service installs its own.
	Raft raft.Config
	// MaxSessions bounds how many clients are remembered for deduplication.
	// <= 0 selects DefaultMaxSessions.
	MaxSessions int
	// SnapshotThreshold is how many applied entries trigger a snapshot and
	// log compaction. 0 selects DefaultSnapshotThreshold.
	SnapshotThreshold uint64
	// DisableSnapshots stops this node from creating snapshots, so its log
	// grows without bound. It can still restore snapshots sent by a leader.
	DisableSnapshots bool
}

// Service is one node's replicated key-value service. Writes go through
// Raft; reads are linearizable via ReadIndex. Only the leader serves
// requests: other nodes return *NotLeaderError.
type Service struct {
	node    *raft.Node
	machine *Machine
	st      *store.Store

	closed    chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex // guards waiters; held while proposing so Apply cannot overtake registration
	waiters map[uint64]*waiter
}

type outcome struct {
	term uint64 // term of the entry that was actually applied at the index
	res  ApplyResult
}

// waiter is a write waiting for its log entry to be applied.
type waiter struct {
	done chan outcome
}

// New builds a service. Call Start to run it.
func New(cfg Config) (*Service, error) {
	if cfg.Raft.Apply != nil || cfg.Raft.Snapshot != nil || cfg.Raft.Restore != nil || cfg.Raft.SnapshotThreshold != 0 {
		return nil, fmt.Errorf("%w: kv.Config.Raft.Apply, Snapshot, Restore and SnapshotThreshold must be unset; the service installs its own",
			raft.ErrInvalidConfig)
	}
	st := store.New()
	s := &Service{
		st:      st,
		machine: NewMachine(st, cfg.MaxSessions),
		closed:  make(chan struct{}),
		waiters: make(map[uint64]*waiter),
	}
	rcfg := cfg.Raft
	rcfg.Apply = s.apply
	rcfg.Restore = s.machine.Restore
	if !cfg.DisableSnapshots {
		rcfg.Snapshot = s.machine.Snapshot
		rcfg.SnapshotThreshold = cfg.SnapshotThreshold
		if rcfg.SnapshotThreshold == 0 {
			rcfg.SnapshotThreshold = DefaultSnapshotThreshold
		}
	}
	node, err := raft.NewNode(rcfg)
	if err != nil {
		st.Close()
		return nil, err
	}
	s.node = node
	return s, nil
}

// Node exposes the underlying Raft node (status, tests, RPC wiring).
func (s *Service) Node() *raft.Node { return s.node }

// Start launches the Raft node.
func (s *Service) Start() { s.node.Start() }

// Stop shuts the service down and releases waiting requests. It is idempotent.
func (s *Service) Stop() {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.node.Stop()
		s.st.Close()
	})
}

// StaleGet reads this node's local state with NO consistency guarantee: it
// works on followers and may lag behind or, on a deposed leader, be outdated.
// It exists for diagnostics and tests.
func (s *Service) StaleGet(key string) ([]byte, bool) { return s.machine.Get(key) }

// Set stores value under key. ttl <= 0 means no expiry. It returns once the
// write is committed and applied. clientID and requestID (>= 1) identify the
// request: retrying a write with the same pair is safe and never applies it twice.
func (s *Service) Set(ctx context.Context, clientID string, requestID uint64, key string, value []byte, ttl time.Duration) error {
	cmd := store.Command{Op: store.OpSet, Key: key, Value: value}
	if ttl > 0 {
		cmd.ExpiresAt = time.Now().Add(ttl).UnixNano() // absolute, so every replica agrees
	}
	_, err := s.submit(ctx, clientID, requestID, cmd)
	return err
}

// Delete removes key and reports whether a live key was removed. The same
// exactly-once rules as Set apply, including for the returned boolean.
func (s *Service) Delete(ctx context.Context, clientID string, requestID uint64, key string) (bool, error) {
	res, err := s.submit(ctx, clientID, requestID, store.Command{Op: store.OpDelete, Key: key})
	return res.Deleted, err
}

// Get is a linearizable read: it reflects every write acknowledged before
// the call began. Only the leader can serve it.
func (s *Service) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, store.ErrEmptyKey
	}
	index, err := s.readIndex(ctx)
	if err != nil {
		return nil, false, err
	}
	if err := s.waitApplied(ctx, index); err != nil {
		return nil, false, err
	}
	v, ok := s.machine.Get(key)
	return v, ok, nil
}

// ---- internals ----------------------------------------------------------------

// apply is the raft apply callback: run the entry, then wake its writer.
func (s *Service) apply(e raft.Entry) {
	res := s.machine.Apply(e)

	s.mu.Lock()
	w := s.waiters[e.Index]
	if w != nil {
		delete(s.waiters, e.Index)
	}
	s.mu.Unlock()

	if w != nil {
		w.done <- outcome{term: e.Term, res: res} // buffered: never blocks
	}
}

func (s *Service) submit(ctx context.Context, clientID string, requestID uint64, cmd store.Command) (ApplyResult, error) {
	if clientID == "" || requestID == 0 {
		return ApplyResult{}, fmt.Errorf("%w: client_id and request_id (>= 1) are required", ErrInvalidRequest)
	}
	if cmd.Key == "" {
		return ApplyResult{}, store.ErrEmptyKey
	}

	// Fast path: this request was already applied (a retry), so answer from
	// the session table instead of growing the log.
	if res, ok := s.machine.Lookup(clientID, requestID); ok {
		if err := res.failure(); err != nil {
			return ApplyResult{}, err
		}
		return res, nil
	}

	data := Command{ClientID: clientID, RequestID: requestID, Cmd: cmd}.Encode()

	// Holding s.mu across Propose and registration guarantees that Apply
	// cannot deliver this entry before the waiter exists.
	s.mu.Lock()
	index, term, err := s.node.Propose(data)
	if err != nil {
		s.mu.Unlock()
		return ApplyResult{}, s.mapRaft(err)
	}
	w := &waiter{done: make(chan outcome, 1)}
	s.waiters[index] = w
	s.mu.Unlock()

	return s.await(ctx, index, term, w)
}

// await blocks until the entry we proposed is applied, or we learn that it
// will not be, or the caller gives up.
func (s *Service) await(ctx context.Context, index, term uint64, w *waiter) (ApplyResult, error) {
	tick := time.NewTicker(leaderCheckInterval)
	defer tick.Stop()

	for {
		select {
		case out := <-w.done:
			return resolve(out, term)
		case <-ctx.Done():
			s.dropWaiter(index, w)
			return ApplyResult{}, ctx.Err() // outcome unknown: the entry may still commit
		case <-s.closed:
			s.dropWaiter(index, w)
			return ApplyResult{}, ErrStopped
		case <-tick.C:
			if st := s.node.Status(); st.State != raft.Leader || st.Term != term {
				s.dropWaiter(index, w)
				select {
				case out := <-w.done: // it was applied just before we noticed
					return resolve(out, term)
				default:
					return ApplyResult{}, ErrLeadershipLost
				}
			}
		}
	}
}

func resolve(out outcome, term uint64) (ApplyResult, error) {
	if out.term != term {
		return ApplyResult{}, ErrOverwritten
	}
	if err := out.res.failure(); err != nil {
		return ApplyResult{}, err
	}
	return out.res, nil
}

func (s *Service) dropWaiter(index uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[index] == w {
		delete(s.waiters, index)
	}
	s.mu.Unlock()
}

// readIndex asks Raft to confirm leadership, retrying briefly while a new
// leader is still committing its first entry.
func (s *Service) readIndex(ctx context.Context) (uint64, error) {
	for {
		index, err := s.node.ReadIndex(ctx)
		switch {
		case err == nil:
			return index, nil
		case errors.Is(err, raft.ErrNotReady):
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-s.closed:
				return 0, ErrStopped
			case <-time.After(notReadyRetry):
			}
		default:
			return 0, s.mapRaft(err)
		}
	}
}

// waitApplied blocks until the state machine has applied everything up to index.
func (s *Service) waitApplied(ctx context.Context, index uint64) error {
	if s.node.Status().Applied >= index {
		return nil
	}
	t := time.NewTicker(applyPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			return ErrStopped
		case <-t.C:
			if s.node.Status().Applied >= index {
				return nil
			}
		}
	}
}

func (s *Service) mapRaft(err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return &NotLeaderError{LeaderID: s.node.Status().LeaderID}
	case errors.Is(err, raft.ErrStopped):
		return ErrStopped
	}
	return err
}
