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

var (
	// ErrCorruptStorage means persisted state is internally inconsistent.
	ErrCorruptStorage = errors.New("raft: corrupt storage")
	// ErrOutOfOrder is returned when appended entries do not extend the log.
	ErrOutOfOrder = errors.New("raft: storage: non-contiguous append")
)

// Storage persists a node's hard state and log. Every method that returns
// nil must have made its change durable: Raft replies to RPCs only after
// these calls succeed.
//
// Entries are numbered from 1. Implementations need not be safe for
// concurrent use by several nodes, but a node may call them from several
// goroutines (always serialised by the node).
type Storage interface {
	// Load returns the persisted hard state and all log entries (index 1..N).
	Load() (HardState, []Entry, error)
	// SaveHardState durably replaces the hard state.
	SaveHardState(hs HardState) error
	// Append durably adds entries after the current last entry.
	Append(entries []Entry) error
	// TruncateFrom durably removes every entry with index >= index.
	TruncateFrom(index uint64) error
	// Close releases resources. It is safe to call more than once.
	Close() error
}

// MemStorage is an in-memory Storage. It is not durable across process
// restarts, but a MemStorage that outlives a Node lets tests simulate a node
// crashing and coming back with its disk intact.
type MemStorage struct {
	mu      sync.Mutex
	hs      HardState
	entries []Entry
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

// Entries returns a copy of the stored log.
func (m *MemStorage) Entries() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Entry(nil), m.entries...)
}

// Load implements Storage.
func (m *MemStorage) Load() (HardState, []Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, append([]Entry(nil), m.entries...), nil
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
	want := uint64(len(m.entries)) + 1
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
	if index <= uint64(len(m.entries)) {
		m.entries = m.entries[:index-1]
	}
	return nil
}

// Close implements Storage. It does nothing.
func (m *MemStorage) Close() error { return nil }
