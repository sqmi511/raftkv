package store

import (
	"testing"
	"time"
)

func TestApplySetAndDelete(t *testing.T) {
	s, _ := newTestStore(t)

	if s.Apply(Command{Op: OpSet, Key: "k", Value: []byte("v")}) {
		t.Fatal("a Set must not report a deletion")
	}
	if got, ok := s.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("Get(k) = %q, %v; want v", got, ok)
	}
	if !s.Apply(Command{Op: OpDelete, Key: "k"}) {
		t.Fatal("Delete of a live key should report true")
	}
	if s.Apply(Command{Op: OpDelete, Key: "k"}) {
		t.Fatal("Delete of a missing key should report false")
	}
}

func TestApplyAbsoluteExpiry(t *testing.T) {
	s, clk := newTestStore(t)

	s.Apply(Command{Op: OpSet, Key: "k", Value: []byte("v"), ExpiresAt: clk.Now().Add(10 * time.Second).UnixNano()})

	clk.Advance(9 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired too early")
	}
	clk.Advance(time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should have expired at its absolute deadline")
	}
}

func TestApplyAlreadyExpiredSetDeletesOlderValue(t *testing.T) {
	s, clk := newTestStore(t)

	s.Set("k", []byte("old"), 0)
	s.Apply(Command{Op: OpSet, Key: "k", Value: []byte("new"), ExpiresAt: clk.Now().Add(-time.Second).UnixNano()})

	if got, ok := s.Get("k"); ok {
		t.Fatalf("Get(k) = %q, true; want missing (old value must not come back)", got)
	}
}

func TestApplyIgnoresUnknownOp(t *testing.T) {
	s, _ := newTestStore(t)

	if s.Apply(Command{Op: 99, Key: "k"}) {
		t.Fatal("unknown op reported a deletion")
	}
	if s.Len() != 0 {
		t.Fatal("unknown op changed the store")
	}
}
