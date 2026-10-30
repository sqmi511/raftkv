package raft

import (
	"errors"
	"fmt"
	"sync"
)

// HardState is the state, besides the log, that must survive a restart.
// Forgetting it could make a node vote twice in the same term.
type HardState struct {
	Term     uint64
	VotedFor string
}

// Snapshot is a point-in-time image of the state machine. It stands in for
// every log entry with index <= Index. Index 0 means "no snapshot".
type Snapshot struct {
	Index uint64 // last log index the snapshot covers
	Term  uint64 // term of that entry
	Data  []byte // opaque, produced and consumed by the state machine
}

var (
	// ErrCorruptStorage means persisted state is internally inconsistent.
	ErrCorruptStorage = errors.New("raft: corrupt storage")
	// ErrOutOfOrder is returned when appended entries do not extend the log.
	ErrOutOfOrder = errors.New("raft: storage: non-contiguous append")
)

// Storage persists a node's hard state, snapshot and log. Every method that
// returns nil must have made its change durable: Raft replies to RPCs only
// after these calls succeed.
//
// Log entries are numbered from 1. Once a snapshot exists, the stored log
// holds only the entries after snapshot.Index.
//
// Implementations need not be safe for concurrent use by several nodes, but
// a node may call them from several goroutines (always serialised by the node).
type Storage interface {
	// Load returns the persisted hard state and the log entries that follow
	// the snapshot (index snapshot.Index+1 .. N).
	Load() (HardState, []Entry, error)
	// LoadSnapshot returns the latest snapshot, or the zero Snapshot if none.
	LoadSnapshot() (Snapshot, error)
	// SaveHardState durably replaces the hard state.
	SaveHardState(hs HardState) error
	// Append durably adds entries after the current last entry.
	Append(entries []Entry) error
	// TruncateFrom durably removes every entry with index >= index.
	TruncateFrom(index uint64) error
	// SaveSnapshot durably replaces the snapshot and discards the log
	// entries it covers. Entries after snap.Index are kept only if the log
	// holds an entry at snap.Index with term snap.Term; otherwise the whole
	// log is discarded. snap.Index must be newer than the current snapshot.
	SaveSnapshot(snap Snapshot) error
	// Close releases resources. It is safe to call more than once.
	Close() error
}

// suffixAfter returns the entries of all (a contiguous run) that remain once
// snap is installed, following the rule documented on Storage.SaveSnapshot.
func suffixAfter(all []Entry, snap Snapshot) ([]Entry, error) {
	if len(all) == 0 {
		return nil, nil
	}
	first, last := all[0].Index, all[len(all)-1].Index
	if first > snap.Index+1 {
		return nil, fmt.Errorf("%w: log starts at index %d but the snapshot ends at %d",
			ErrCorruptStorage, first, snap.Index)
	}
	if snap.Index == 0 {
		return all, nil
	}
	if last < snap.Index {
		return nil, nil // the snapshot is ahead of the whole log
	}
	if first <= snap.Index {
		if all[snap.Index-first].Term != snap.Term {
			return nil, nil // the log conflicts with the snapshot
		}
		return all[snap.Index-first+1:], nil
	}
	return all, nil // already compacted: the log starts right after the snapshot
}

// MemStorage is an in-memory Storage. It is not durable across process
// restarts, but a MemStorage that outlives a Node lets tests simulate a node
// crashing and coming back with its disk intact.
type MemStorage struct {
	mu      sync.Mutex
	hs      HardState
	snap    Snapshot
	entries []Entry // entries[i] has index snap.Index+1+i
	err     error
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// SetError makes every subsequent write fail with err (nil restores normal
// behaviour). It is used to test how a node reacts to a failing disk.
func (m *MemStorage) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Entries returns a copy of the stored log (the entries after the snapshot).
func (m *MemStorage) Entries() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Entry(nil), m.entries...)
}

// Snapshot returns the stored snapshot (the zero value if none).
func (m *MemStorage) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap
}

// Load implements Storage.
func (m *MemStorage) Load() (HardState, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, append([]Entry(nil), m.entries...), nil
}

// LoadSnapshot implements Storage.
func (m *MemStorage) LoadSnapshot() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, nil
}

// SaveHardState implements Storage.
func (m *MemStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.hs = hs
	return nil
}

// Append implements Storage.
func (m *MemStorage) Append(entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	want := m.snap.Index + uint64(len(m.entries)) + 1
	for i, e := range entries {
		if e.Index != want+uint64(i) {
			return fmt.Errorf("%w: entry index %d, want %d", ErrOutOfOrder, e.Index, want+uint64(i))
		}
	}
	m.entries = append(m.entries, entries...)
	return nil
}

// TruncateFrom implements Storage.
func (m *MemStorage) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if index < 1 {
		return fmt.Errorf("raft: storage: bad truncate index %d", index)
	}
	if index <= m.snap.Index {
		return fmt.Errorf("raft: storage: cannot truncate index %d inside snapshot %d", index, m.snap.Index)
	}
	if pos := index - m.snap.Index - 1; pos < uint64(len(m.entries)) {
		m.entries = m.entries[:pos]
	}
	return nil
}

// SaveSnapshot implements Storage.
func (m *MemStorage) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	if s.Index <= m.snap.Index {
		return fmt.Errorf("raft: storage: snapshot index %d is not newer than %d", s.Index, m.snap.Index)
	}
	kept, err := suffixAfter(m.entries, s)
	if err != nil {
		return err
	}
	m.entries = append([]Entry(nil), kept...)
	m.snap = Snapshot{Index: s.Index, Term: s.Term, Data: append([]byte(nil), s.Data...)}
	return nil
}

// Close implements Storage. It does nothing.
func (m *MemStorage) Close() error { return nil }
