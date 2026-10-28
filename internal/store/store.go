// Package store implements a thread-safe in-memory key-value store with
// per-key TTL. Expired keys are invisible to readers immediately (lazy
// expiry) and are physically removed by a background sweeper.
package store

import (
	"sync"
	"time"
)

// DefaultSweepInterval is how often the background sweeper runs.
const DefaultSweepInterval = time.Second

type entry struct {
	value     []byte
	expiresAt time.Time // zero value means "never expires"
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the time source (used by tests).
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// WithSweepInterval sets the background sweep period.
// A value <= 0 disables the background sweeper; DeleteExpired can still be
// called manually.
func WithSweepInterval(d time.Duration) Option {
	return func(s *Store) { s.sweepEvery = d }
}

// Store is a concurrency-safe key-value map with optional TTLs.
type Store struct {
	mu         sync.RWMutex
	data       map[string]entry
	now        func() time.Time
	sweepEvery time.Duration

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// New creates a Store and starts its background sweeper (unless disabled).
// Call Close when finished to stop the sweeper goroutine.
func New(opts ...Option) *Store {
	s := &Store{
		data:       make(map[string]entry),
		now:        time.Now,
		sweepEvery: DefaultSweepInterval,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.sweepEvery > 0 {
		go s.janitor()
	} else {
		close(s.done)
	}
	return s
}

// Get returns a copy of the value for key. The boolean is false if the key
// does not exist or has expired.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.expired(s.now()) {
		return nil, false
	}
	return cloneBytes(e.value), true
}

// Set stores a copy of value under key. A ttl <= 0 means the key never
// expires. Setting an existing key replaces both its value and its TTL.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	e := entry{value: cloneBytes(value)}

	s.mu.Lock()
	defer s.mu.Unlock()

	if ttl > 0 {
		e.expiresAt = s.now().Add(ttl)
	}
	s.data[key] = e
}

// Delete removes key. It reports whether a live (non-expired) key was removed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return false
	}
	delete(s.data, key)
	return !e.expired(s.now())
}

// Len returns the number of live (non-expired) keys. It is O(n).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	n := 0
	for _, e := range s.data {
		if !e.expired(now) {
			n++
		}
	}
	return n
}

// DeleteExpired removes all expired keys and returns how many were removed.
func (s *Store) DeleteExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	removed := 0
	for k, e := range s.data {
		if e.expired(now) {
			delete(s.data, k)
			removed++
		}
	}
	return removed
}

// Close stops the background sweeper and waits for it to exit.
// It is safe to call multiple times. The store remains usable afterwards,
// but expired keys are then only hidden, not freed, unless DeleteExpired is
// called manually.
func (s *Store) Close() {
	s.closeOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *Store) janitor() {
	defer close(s.done)

	ticker := time.NewTicker(s.sweepEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.DeleteExpired()
		case <-s.stop:
			return
		}
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
