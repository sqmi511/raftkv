package store

import (
	"sort"
	"time"
)

// Item is one live key in a Dump.
type Item struct {
	Key       string
	Value     []byte
	ExpiresAt int64 // absolute time in unix nanoseconds; 0 means "never expires"
}

// Dump returns every live (non-expired) key, sorted by key, with copies of
// the values. It is the store's half of a state machine snapshot.
func (s *Store) Dump() []Item {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	items := make([]Item, 0, len(s.data))
	for k, e := range s.data {
		if e.expired(now) {
			continue
		}
		it := Item{Key: k, Value: cloneBytes(e.value)}
		if !e.expiresAt.IsZero() {
			it.ExpiresAt = e.expiresAt.UnixNano()
		}
		items = append(items, it)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items
}

// Replace discards the store's entire content and loads items instead. Items
// that have already expired are skipped. Values are copied.
func (s *Store) Replace(items []Item) {
	now := s.now()
	data := make(map[string]entry, len(items))
	for _, it := range items {
		e := entry{value: cloneBytes(it.Value)}
		if it.ExpiresAt != 0 {
			e.expiresAt = time.Unix(0, it.ExpiresAt)
		}
		if e.expired(now) {
			continue
		}
		data[it.Key] = e
	}

	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
}
