package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock, so TTL tests never sleep.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestStore returns a store with a fake clock and the sweeper disabled.
func newTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	s := New(WithClock(clk.Now), WithSweepInterval(0))
	t.Cleanup(s.Close)
	return s, clk
}

// rawLen counts all entries, including expired ones not yet swept.
func (s *Store) rawLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func TestSetGet(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set("k", []byte("v"), 0)
	got, ok := s.Get("k")
	if !ok || string(got) != "v" {
		t.Fatalf("Get(k) = %q, %v; want %q, true", got, ok, "v")
	}
}

func TestGetMissing(t *testing.T) {
	s, _ := newTestStore(t)

	if v, ok := s.Get("nope"); ok || v != nil {
		t.Fatalf("Get(nope) = %q, %v; want nil, false", v, ok)
	}
}

func TestOverwrite(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set("k", []byte("a"), 0)
	s.Set("k", []byte("b"), 0)
	got, _ := s.Get("k")
	if string(got) != "b" {
		t.Fatalf("got %q, want %q", got, "b")
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
}

func TestEmptyValueIsStoredAndFound(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set("k", []byte{}, 0)
	got, ok := s.Get("k")
	if !ok || len(got) != 0 {
		t.Fatalf("Get(k) = %v, %v; want empty value, true", got, ok)
	}
}

func TestDelete(t *testing.T) {
	s, _ := newTestStore(t)

	s.Set("k", []byte("v"), 0)
	if !s.Delete("k") {
		t.Fatal("Delete(existing) = false, want true")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key still present after Delete")
	}
	if s.Delete("k") {
		t.Fatal("Delete(missing) = true, want false")
	}
}

func TestTTLExpiry(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("k", []byte("v"), 10*time.Second)

	clk.Advance(9 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired too early")
	}

	clk.Advance(1 * time.Second) // exactly at expiry: considered expired
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should be expired at its TTL boundary")
	}
}

func TestNoTTLNeverExpires(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("zero", []byte("v"), 0)
	s.Set("neg", []byte("v"), -5*time.Second)
	clk.Advance(1000 * time.Hour)

	for _, k := range []string{"zero", "neg"} {
		if _, ok := s.Get(k); !ok {
			t.Fatalf("key %q expired but had no TTL", k)
		}
	}
}

func TestSetReplacesTTL(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("k", []byte("v1"), 5*time.Second)
	s.Set("k", []byte("v2"), 0) // overwrite without TTL clears the old TTL
	clk.Advance(time.Hour)
	if got, ok := s.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("Get(k) = %q, %v; want v2, true", got, ok)
	}

	s.Set("k", []byte("v3"), 5*time.Second)
	clk.Advance(4 * time.Second)
	s.Set("k", []byte("v4"), 5*time.Second) // TTL restarts from now
	clk.Advance(4 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired; TTL should have been reset by second Set")
	}
}

func TestDeleteExpiredKeyReturnsFalse(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("k", []byte("v"), time.Second)
	clk.Advance(2 * time.Second)
	if s.Delete("k") {
		t.Fatal("Delete of an expired key should report false")
	}
	if s.rawLen() != 0 {
		t.Fatal("expired key should still be physically removed by Delete")
	}
}

func TestLenIgnoresExpired(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("a", []byte("1"), time.Second)
	s.Set("b", []byte("2"), 0)
	s.Set("c", []byte("3"), time.Hour)

	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	clk.Advance(2 * time.Second)
	if s.Len() != 2 {
		t.Fatalf("Len after expiry = %d, want 2", s.Len())
	}
}

func TestDeleteExpired(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("a", []byte("1"), time.Second)
	s.Set("b", []byte("2"), time.Second)
	s.Set("c", []byte("3"), time.Hour)
	s.Set("d", []byte("4"), 0)

	if n := s.DeleteExpired(); n != 0 {
		t.Fatalf("DeleteExpired before expiry = %d, want 0", n)
	}

	clk.Advance(2 * time.Second)
	if s.rawLen() != 4 {
		t.Fatalf("rawLen = %d, want 4 (not swept yet)", s.rawLen())
	}
	if n := s.DeleteExpired(); n != 2 {
		t.Fatalf("DeleteExpired = %d, want 2", n)
	}
	if s.rawLen() != 2 {
		t.Fatalf("rawLen after sweep = %d, want 2", s.rawLen())
	}
}

func TestValuesAreCopied(t *testing.T) {
	s, _ := newTestStore(t)

	in := []byte("hello")
	s.Set("k", in, 0)
	in[0] = 'X' // mutating the caller's slice must not affect the store

	got, _ := s.Get("k")
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("store aliased the input slice: got %q", got)
	}

	got[0] = 'Y' // mutating the returned slice must not affect the store
	again, _ := s.Get("k")
	if !bytes.Equal(again, []byte("hello")) {
		t.Fatalf("store aliased the returned slice: got %q", again)
	}
}

func TestBackgroundSweeperRemovesExpired(t *testing.T) {
	s := New(WithSweepInterval(5 * time.Millisecond))
	defer s.Close()

	s.Set("a", []byte("1"), 10*time.Millisecond)
	s.Set("keep", []byte("2"), 0)

	deadline := time.Now().Add(2 * time.Second)
	for s.rawLen() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("sweeper did not remove expired key; rawLen = %d", s.rawLen())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := s.Get("keep"); !ok {
		t.Fatal("non-expiring key was removed")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s := New(WithSweepInterval(time.Millisecond))
	s.Close()
	s.Close() // must not panic or block

	s2 := New(WithSweepInterval(0))
	s2.Close()
	s2.Close()
}

func TestConcurrentAccess(t *testing.T) {
	s := New(WithSweepInterval(time.Millisecond))
	defer s.Close()

	const (
		workers = 16
		ops     = 500
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k%d", i%20)
				switch i % 4 {
				case 0:
					s.Set(key, []byte(fmt.Sprintf("w%d-%d", w, i)), time.Millisecond)
				case 1:
					s.Set(key, []byte("x"), 0)
				case 2:
					s.Get(key)
				case 3:
					s.Delete(key)
				}
				if i%50 == 0 {
					s.Len()
				}
			}
		}(w)
	}
	wg.Wait()
}
