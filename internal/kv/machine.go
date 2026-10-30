// Package kv is the replicated key-value service: a state machine driven by
// the Raft log, exactly-once request handling, and linearizable reads.
package kv

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/store"
)

// Command is what a client write turns into inside the Raft log: the plain
// store command plus the identity of the request, used for deduplication.
//
// Wire format:
//
//	uvarint(len(ClientID)) | ClientID | uvarint(RequestID) | store.Command bytes
type Command struct {
	ClientID  string
	RequestID uint64
	Cmd       store.Command
}

// Encode serializes the command.
func (c Command) Encode() []byte {
	body := c.Cmd.Encode()
	buf := make([]byte, 0, 2*binary.MaxVarintLen64+len(c.ClientID)+len(body))
	buf = binary.AppendUvarint(buf, uint64(len(c.ClientID)))
	buf = append(buf, c.ClientID...)
	buf = binary.AppendUvarint(buf, c.RequestID)
	return append(buf, body...)
}

// DecodeCommand parses a serialized command. Errors wrap store.ErrBadCommand.
func DecodeCommand(b []byte) (Command, error) {
	idLen, n := binary.Uvarint(b)
	if n <= 0 {
		return Command{}, fmt.Errorf("%w: bad client id length", store.ErrBadCommand)
	}
	b = b[n:]
	if idLen > uint64(len(b)) {
		return Command{}, fmt.Errorf("%w: client id length %d exceeds record", store.ErrBadCommand, idLen)
	}
	clientID := string(b[:idLen])
	b = b[idLen:]

	reqID, n := binary.Uvarint(b)
	if n <= 0 {
		return Command{}, fmt.Errorf("%w: bad request id", store.ErrBadCommand)
	}
	inner, err := store.DecodeCommand(b[n:])
	if err != nil {
		return Command{}, err
	}
	return Command{ClientID: clientID, RequestID: reqID, Cmd: inner}, nil
}

// DefaultMaxSessions bounds the number of remembered clients.
const DefaultMaxSessions = 10_000

// ApplyResult is the outcome of applying one log entry.
type ApplyResult struct {
	Deleted   bool  // a live key was removed (OpDelete only)
	Duplicate bool  // this request was applied before; Deleted is the remembered outcome
	Stale     bool  // the request ID is older than the client's latest; nothing was done
	Err       error // the entry could not be decoded; nothing was done
}

// failure converts the result into an error, if it represents one.
func (r ApplyResult) failure() error {
	switch {
	case r.Err != nil:
		return r.Err
	case r.Stale:
		return ErrStaleRequest
	}
	return nil
}

// session remembers the last request of one client.
type session struct {
	lastRequest uint64
	lastIndex   uint64 // log index of that request: a deterministic age for eviction
	deleted     bool   // result of that request
}

// Machine is the replicated state machine: a store plus the session table
// that makes writes exactly-once. Because the session table is changed only
// by applying log entries, every replica holds the same one, so a retry that
// reaches a NEW leader is still recognised as a duplicate.
type Machine struct {
	mu          sync.Mutex
	st          *store.Store
	sessions    map[string]*session
	maxSessions int
}

// NewMachine wraps st. maxSessions <= 0 selects DefaultMaxSessions.
func NewMachine(st *store.Store, maxSessions int) *Machine {
	if maxSessions <= 0 {
		maxSessions = DefaultMaxSessions
	}
	return &Machine{st: st, sessions: make(map[string]*session), maxSessions: maxSessions}
}

// Get reads from the local store. It is only linearizable when the caller
// has first waited for a ReadIndex (see Service.Get).
func (m *Machine) Get(key string) ([]byte, bool) { return m.st.Get(key) }

// Sessions returns how many clients are currently remembered.
func (m *Machine) Sessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// Lookup reports whether a request was already handled. When ok is true the
// result is either the remembered outcome (Duplicate) or Stale.
func (m *Machine) Lookup(clientID string, requestID uint64) (ApplyResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lookupLocked(clientID, requestID)
}

func (m *Machine) lookupLocked(clientID string, requestID uint64) (ApplyResult, bool) {
	s := m.sessions[clientID]
	if s == nil {
		return ApplyResult{}, false
	}
	switch {
	case requestID == s.lastRequest:
		return ApplyResult{Deleted: s.deleted, Duplicate: true}, true
	case requestID < s.lastRequest:
		return ApplyResult{Stale: true}, true
	}
	return ApplyResult{}, false
}

// Apply executes one committed log entry. It must be deterministic: every
// replica applies the same entries and must reach the same state. An entry
// that cannot be decoded is skipped on every replica alike.
func (m *Machine) Apply(e raft.Entry) ApplyResult {
	cmd, err := DecodeCommand(e.Data)
	if err != nil {
		return ApplyResult{Err: err}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if res, ok := m.lookupLocked(cmd.ClientID, cmd.RequestID); ok {
		return res
	}
	deleted := m.st.Apply(cmd.Cmd)

	s := m.sessions[cmd.ClientID]
	if s == nil {
		s = &session{}
		m.sessions[cmd.ClientID] = s
		if len(m.sessions) > m.maxSessions {
			m.evictOldestLocked(cmd.ClientID)
		}
	}
	s.lastRequest, s.lastIndex, s.deleted = cmd.RequestID, e.Index, deleted
	return ApplyResult{Deleted: deleted}
}

// evictOldestLocked forgets the client whose last request has the lowest log
// index. Log indexes are identical on every replica, so every replica evicts
// the same client. A client evicted this way loses exactly-once protection
// for a retry of its very last request.
func (m *Machine) evictOldestLocked(keep string) {
	var oldest string
	var oldestIndex uint64
	found := false
	for id, s := range m.sessions {
		if id == keep {
			continue
		}
		if !found || s.lastIndex < oldestIndex {
			oldest, oldestIndex, found = id, s.lastIndex, true
		}
	}
	if found {
		delete(m.sessions, oldest)
	}
}
