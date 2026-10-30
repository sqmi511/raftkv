package kv

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/store"
)

func populatedMachine(t *testing.T) *Machine {
	t.Helper()
	m := newTestMachine(t, 0)
	m.Apply(entryFor(1, "c1", 1, setC("a", "1")))
	m.Apply(entryFor(2, "c1", 2, setC("b", "2")))
	m.Apply(entryFor(3, "c2", 1, setC("gone", "x")))
	m.Apply(entryFor(4, "c2", 2, delC("gone"))) // c2's last request removed a live key
	m.Apply(entryFor(5, "c3", 1, store.Command{
		Op: store.OpSet, Key: "ttl", Value: []byte("t"), ExpiresAt: time.Now().Add(time.Hour).UnixNano(),
	}))
	return m
}

func TestMachineSnapshotRoundTrip(t *testing.T) {
	src := populatedMachine(t)
	data, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := src.Snapshot(); !bytes.Equal(data, again) {
		t.Fatal("two snapshots of the same state differ: the encoding must be deterministic")
	}

	dst := newTestMachine(t, 0)
	dst.Apply(entryFor(1, "old", 1, setC("stale", "x"))) // must disappear on restore
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}

	mustValue(t, dst, "a", "1")
	mustValue(t, dst, "b", "2")
	mustValue(t, dst, "ttl", "t")
	if _, ok := dst.Get("stale"); ok {
		t.Fatal("Restore kept state that was not in the snapshot")
	}
	if _, ok := dst.Get("gone"); ok {
		t.Fatal("a deleted key came back")
	}
	if got := dst.Sessions(); got != 3 {
		t.Fatalf("Sessions = %d, want 3 (the old session must be gone)", got)
	}

	// The session table survived: a retried Delete returns its ORIGINAL answer.
	res := dst.Apply(entryFor(6, "c2", 2, delC("gone")))
	if !res.Duplicate || !res.Deleted {
		t.Fatalf("retried Delete = %+v, want Duplicate with Deleted", res)
	}
	// And an old request is still recognised as stale.
	res = dst.Apply(entryFor(7, "c1", 1, setC("a", "overwritten")))
	if !res.Stale {
		t.Fatalf("old request = %+v, want Stale", res)
	}
	mustValue(t, dst, "a", "1")
}

func TestSnapshotOfEmptyMachine(t *testing.T) {
	data, err := newTestMachine(t, 0).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	dst := populatedMachine(t)
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	if _, ok := dst.Get("a"); ok || dst.Sessions() != 0 {
		t.Fatal("restoring an empty snapshot should empty the machine")
	}
}

func TestRestoreRejectsMalformedSnapshots(t *testing.T) {
	valid, err := populatedMachine(t).Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string][]byte{
		"nil":               nil,
		"unknown version":   {99},
		"only the version":  {snapshotVersion},
		"truncated":         valid[:len(valid)-1],
		"truncated early":   valid[:5],
		"trailing garbage":  append(append([]byte(nil), valid...), 0),
		"absurd item count": {snapshotVersion, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
		"bad delete flag":   append(append([]byte(nil), valid[:len(valid)-1]...), 7),
	}
	for name, b := range cases {
		m := populatedMachine(t)
		if err := m.Restore(b); !errors.Is(err, ErrBadSnapshot) {
			t.Errorf("%s: err = %v, want ErrBadSnapshot", name, err)
			continue
		}
		mustValue(t, m, "a", "1") // a failed restore must not touch the state
		if m.Sessions() != 3 {
			t.Errorf("%s: a failed restore changed the session table", name)
		}
	}
}
