package store

import (
	"testing"
	"time"
)

func TestDumpAndReplace(t *testing.T) {
	src, clk := newTestStore(t)
	src.Set("a", []byte("1"), 0)
	src.Set("b", []byte("2"), time.Hour)
	src.Set("gone", []byte("x"), time.Second)
	clk.Advance(2 * time.Second)

	items := src.Dump()
	if len(items) != 2 || items[0].Key != "a" || items[1].Key != "b" {
		t.Fatalf("Dump = %+v, want live keys a and b in order", items)
	}
	if items[0].ExpiresAt != 0 || items[1].ExpiresAt == 0 {
		t.Fatalf("expiry times wrong: %+v", items)
	}

	dst, dclk := newTestStore(t) // its clock starts before src's, at the base time
	dst.Set("stale", []byte("x"), 0)
	dst.Replace(items)

	if _, ok := dst.Get("stale"); ok {
		t.Fatal("Replace kept a key that was not in the items")
	}
	if got, ok := dst.Get("a"); !ok || string(got) != "1" {
		t.Fatalf("Get(a) = %q, %v; want 1", got, ok)
	}
	if _, ok := dst.Get("gone"); ok {
		t.Fatal("an expired key was dumped")
	}

	// b keeps its absolute deadline (base time + 1h).
	dclk.Advance(59 * time.Minute)
	if _, ok := dst.Get("b"); !ok {
		t.Fatal("b expired too early")
	}
	dclk.Advance(2 * time.Minute)
	if _, ok := dst.Get("b"); ok {
		t.Fatal("b should have expired at its absolute deadline")
	}
}

func TestReplaceSkipsExpiredAndCopiesValues(t *testing.T) {
	s, clk := newTestStore(t)

	value := []byte("abc")
	s.Replace([]Item{
		{Key: "live", Value: value},
		{Key: "dead", Value: []byte("x"), ExpiresAt: clk.Now().Add(-time.Second).UnixNano()},
	})
	value[0] = 'X' // must not leak into the store

	if got, ok := s.Get("live"); !ok || string(got) != "abc" {
		t.Fatalf("Get(live) = %q, %v; want abc", got, ok)
	}
	if _, ok := s.Get("dead"); ok {
		t.Fatal("an already-expired item was loaded")
	}

	// Dump hands out copies too.
	items := s.Dump()
	items[0].Value[0] = 'Y'
	if got, _ := s.Get("live"); string(got) != "abc" {
		t.Fatalf("Dump aliased the stored value: %q", got)
	}
}
