package kv

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/store"
)

func setC(k, v string) store.Command {
	return store.Command{Op: store.OpSet, Key: k, Value: []byte(v)}
}

func delC(k string) store.Command {
	return store.Command{Op: store.OpDelete, Key: k}
}

func entryFor(index uint64, client string, req uint64, c store.Command) raft.Entry {
	return raft.Entry{Index: index, Term: 1, Data: Command{ClientID: client, RequestID: req, Cmd: c}.Encode()}
}

func newTestMachine(t *testing.T, maxSessions int) *Machine {
	t.Helper()
	st := store.New(store.WithSweepInterval(0))
	t.Cleanup(st.Close)
	return NewMachine(st, maxSessions)
}

func mustValue(t *testing.T, m *Machine, key, want string) {
	t.Helper()
	got, ok := m.Get(key)
	if !ok || string(got) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, got, ok, want)
	}
}

// ---- command encoding -------------------------------------------------------

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{ClientID: "c1", RequestID: 1, Cmd: setC("k", "v")},
		{ClientID: "", RequestID: 0, Cmd: delC("k")},
		{
			ClientID:  strings.Repeat("c", 300),
			RequestID: 1 << 40,
			Cmd:       store.Command{Op: store.OpSet, Key: "ключ", Value: []byte{0, 1, 0xFF}, ExpiresAt: 42},
		},
	}
	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("DecodeCommand(%+v): %v", want, err)
		}
		if got.ClientID != want.ClientID || got.RequestID != want.RequestID ||
			got.Cmd.Op != want.Cmd.Op || got.Cmd.Key != want.Cmd.Key ||
			got.Cmd.ExpiresAt != want.Cmd.ExpiresAt || !bytes.Equal(got.Cmd.Value, want.Cmd.Value) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
		}
	}
}

func TestDecodeCommandRejectsMalformed(t *testing.T) {
	valid := Command{ClientID: "client", RequestID: 3, Cmd: setC("key", "val")}.Encode()

	cases := map[string][]byte{
		"nil":                     nil,
		"unterminated varint":     bytes.Repeat([]byte{0xFF}, 11),
		"id longer than record":   {50, 'a'},
		"missing request id":      {1, 'a'},
		"missing inner command":   {1, 'a', 3},
		"truncated inner command": valid[:12], // 8 header bytes + 4 bytes of the inner command
	}
	for name, b := range cases {
		if _, err := DecodeCommand(b); !errors.Is(err, store.ErrBadCommand) {
			t.Errorf("%s: err = %v, want store.ErrBadCommand", name, err)
		}
	}
}

// ---- state machine ------------------------------------------------------------

func TestMachineAppliesCommands(t *testing.T) {
	m := newTestMachine(t, 0)

	res := m.Apply(entryFor(1, "c1", 1, setC("k", "v")))
	if res.Duplicate || res.Stale || res.Err != nil || res.Deleted {
		t.Fatalf("Set result = %+v, want a plain success", res)
	}
	mustValue(t, m, "k", "v")

	if res = m.Apply(entryFor(2, "c1", 2, delC("k"))); !res.Deleted {
		t.Fatalf("Delete of a live key = %+v, want Deleted", res)
	}
	if res = m.Apply(entryFor(3, "c1", 3, delC("k"))); res.Deleted {
		t.Fatalf("Delete of a missing key = %+v, want not Deleted", res)
	}
}

func TestMachineDeduplicatesRetries(t *testing.T) {
	m := newTestMachine(t, 0)

	m.Apply(entryFor(1, "c1", 1, setC("k", "first")))
	// The same request appears in the log again (a client retry), now with a
	// different payload to prove that nothing is executed the second time.
	res := m.Apply(entryFor(2, "c1", 1, setC("k", "second")))
	if !res.Duplicate {
		t.Fatalf("retry result = %+v, want Duplicate", res)
	}
	mustValue(t, m, "k", "first")
}

func TestMachineRemembersDeleteOutcome(t *testing.T) {
	m := newTestMachine(t, 0)

	m.Apply(entryFor(1, "c1", 1, setC("k", "v")))
	if res := m.Apply(entryFor(2, "c1", 2, delC("k"))); !res.Deleted {
		t.Fatalf("first Delete = %+v, want Deleted", res)
	}
	// The retry must report the ORIGINAL outcome even though the key is gone.
	res := m.Apply(entryFor(3, "c1", 2, delC("k")))
	if !res.Duplicate || !res.Deleted {
		t.Fatalf("retried Delete = %+v, want Duplicate with Deleted", res)
	}
}

func TestMachineIgnoresStaleRequests(t *testing.T) {
	m := newTestMachine(t, 0)

	m.Apply(entryFor(1, "c1", 5, setC("k", "newest")))
	res := m.Apply(entryFor(2, "c1", 3, setC("k", "old")))
	if !res.Stale {
		t.Fatalf("old request result = %+v, want Stale", res)
	}
	mustValue(t, m, "k", "newest")
	if !errors.Is(res.failure(), ErrStaleRequest) {
		t.Fatalf("failure() = %v, want ErrStaleRequest", res.failure())
	}
}

func TestMachineSkipsUndecodableEntries(t *testing.T) {
	m := newTestMachine(t, 0)

	res := m.Apply(raft.Entry{Index: 1, Term: 1, Data: []byte{0xFF, 0xFF}})
	if res.Err == nil || res.failure() == nil {
		t.Fatalf("result = %+v, want an Err", res)
	}
	if m.Sessions() != 0 {
		t.Fatal("a bad entry created a session")
	}
}

func TestLookup(t *testing.T) {
	m := newTestMachine(t, 0)
	m.Apply(entryFor(1, "c1", 2, setC("k", "v")))

	if _, ok := m.Lookup("nobody", 1); ok {
		t.Fatal("unknown client reported as known")
	}
	if res, ok := m.Lookup("c1", 2); !ok || !res.Duplicate {
		t.Fatalf("Lookup(latest) = %+v, %v; want Duplicate", res, ok)
	}
	if res, ok := m.Lookup("c1", 1); !ok || !res.Stale {
		t.Fatalf("Lookup(older) = %+v, %v; want Stale", res, ok)
	}
	if _, ok := m.Lookup("c1", 3); ok {
		t.Fatal("a newer request id must not look like a duplicate")
	}
}

func TestSessionEvictionIsDeterministic(t *testing.T) {
	m := newTestMachine(t, 3)

	for i := 1; i <= 4; i++ {
		m.Apply(entryFor(uint64(i), fmt.Sprintf("c%d", i), 1, setC(fmt.Sprintf("k%d", i), "v")))
	}
	if got := m.Sessions(); got != 3 {
		t.Fatalf("Sessions = %d, want 3", got)
	}
	if _, ok := m.Lookup("c1", 1); ok {
		t.Fatal("the session with the oldest log index should have been evicted")
	}
	for _, id := range []string{"c2", "c3", "c4"} {
		if _, ok := m.Lookup(id, 1); !ok {
			t.Fatalf("session %s was evicted but should have been kept", id)
		}
	}
	mustValue(t, m, "k1", "v") // evicting a session never touches data
}
