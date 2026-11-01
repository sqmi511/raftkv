package shard_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/sqmi511/raftkv/internal/shard"
)

func newRing(t *testing.T, names ...string) *shard.Ring {
	t.Helper()
	r := shard.NewRing(0)
	for _, n := range names {
		if err := r.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func locate(t *testing.T, r *shard.Ring, key string) string {
	t.Helper()
	g, err := r.Locate(key)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestEmptyAndInvalid(t *testing.T) {
	r := shard.NewRing(0)
	if _, err := r.Locate("k"); !errors.Is(err, shard.ErrEmptyRing) {
		t.Fatalf("Locate on an empty ring = %v, want ErrEmptyRing", err)
	}
	if err := r.Add(""); !errors.Is(err, shard.ErrInvalidGroup) {
		t.Fatalf("Add(\"\") = %v, want ErrInvalidGroup", err)
	}
	if err := r.Add("a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Add("a"); !errors.Is(err, shard.ErrInvalidGroup) {
		t.Fatalf("duplicate Add = %v, want ErrInvalidGroup", err)
	}
	if r.Remove("nope") {
		t.Fatal("Remove of an unknown group reported true")
	}
	if !r.Remove("a") {
		t.Fatal("Remove of a known group reported false")
	}
	if _, err := r.Locate("k"); !errors.Is(err, shard.ErrEmptyRing) {
		t.Fatalf("Locate after removing every group = %v, want ErrEmptyRing", err)
	}
}

func TestSingleGroupOwnsEverything(t *testing.T) {
	r := newRing(t, "only")
	for i := 0; i < 200; i++ {
		if g := locate(t, r, fmt.Sprintf("k%d", i)); g != "only" {
			t.Fatalf("key went to %q", g)
		}
	}
}

func TestLocateIsDeterministicAndIndependentOfInsertionOrder(t *testing.T) {
	a := newRing(t, "g1", "g2", "g3")
	b := newRing(t, "g3", "g1", "g2")
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("key-%d", i)
		ga, gb := locate(t, a, key), locate(t, b, key)
		if ga != gb || ga != locate(t, a, key) {
			t.Fatalf("key %q: %q vs %q", key, ga, gb)
		}
	}
}

func TestKeysAreSpreadEvenly(t *testing.T) {
	r := newRing(t, "g0", "g1", "g2")
	const n = 10000
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		counts[locate(t, r, fmt.Sprintf("key-%d", i))]++
	}
	for _, g := range r.Groups() {
		if share := float64(counts[g]) / n; share < 0.20 || share > 0.48 {
			t.Errorf("group %s owns %.1f%% of the keys, want roughly a third (counts %v)", g, share*100, counts)
		}
	}
}

func TestAddingAGroupOnlyMovesKeysToIt(t *testing.T) {
	before := newRing(t, "g0", "g1", "g2")
	after := newRing(t, "g0", "g1", "g2", "g3")
	const n = 10000
	moved := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		was, now := locate(t, before, key), locate(t, after, key)
		if was == now {
			continue
		}
		moved++
		if now != "g3" {
			t.Fatalf("key %q moved from %s to %s: keys may only move to the new group", key, was, now)
		}
	}
	// Ideally 1/4 of the keys move; a modulo scheme would move about 3/4.
	if frac := float64(moved) / n; frac < 0.15 || frac > 0.35 {
		t.Fatalf("%.1f%% of the keys moved, want about 25%%", frac*100)
	}
}

func TestRemovingAGroupOnlyMovesItsKeys(t *testing.T) {
	r := newRing(t, "g0", "g1", "g2", "g3")
	const n = 5000
	owners := make([]string, n)
	for i := range owners {
		owners[i] = locate(t, r, fmt.Sprintf("key-%d", i))
	}
	if !r.Remove("g2") {
		t.Fatal("Remove failed")
	}
	for i := range owners {
		now := locate(t, r, fmt.Sprintf("key-%d", i))
		switch {
		case now == "g2":
			t.Fatal("a removed group still owns keys")
		case owners[i] != "g2" && now != owners[i]:
			t.Fatalf("key-%d moved from %s to %s although its group was not removed", i, owners[i], now)
		}
	}
	if got := r.Groups(); !reflect.DeepEqual(got, []string{"g0", "g1", "g3"}) {
		t.Fatalf("Groups() = %v", got)
	}
}

func TestParseGroups(t *testing.T) {
	got, err := shard.ParseGroups(" users = a:1, b:2 ;orders=c:3;; ")
	if err != nil {
		t.Fatal(err)
	}
	want := []shard.Group{{Name: "users", Addrs: []string{"a:1", "b:2"}}, {Name: "orders", Addrs: []string{"c:3"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	for _, bad := range []string{"", " ; ", "noequals", "=a:1", "a=", "a= , ", "a=x:1;a=y:2"} {
		if _, err := shard.ParseGroups(bad); !errors.Is(err, shard.ErrInvalidGroup) {
			t.Errorf("ParseGroups(%q) = %v, want ErrInvalidGroup", bad, err)
		}
	}
}
