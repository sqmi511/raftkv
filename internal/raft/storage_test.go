package raft_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/sqmi511/raftkv/internal/raft"
)

// storageContract checks behaviour every Storage implementation must share.
func storageContract(t *testing.T, s raft.Storage) {
	t.Helper()

	hs, entries, err := s.Load()
	if err != nil || hs != (raft.HardState{}) || len(entries) != 0 {
		t.Fatalf("fresh storage Load = %+v, %v, %v; want empty", hs, entries, err)
	}

	if err := s.SaveHardState(raft.HardState{Term: 3, VotedFor: "n2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(raft.HardState{Term: 4}); err != nil { // latest wins
		t.Fatal(err)
	}

	if err := s.Append([]raft.Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]raft.Entry{{Index: 3, Term: 2, Data: []byte("c")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(nil); err != nil { // empty append is a no-op
		t.Fatal(err)
	}
	if err := s.TruncateFrom(3); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]raft.Entry{{Index: 3, Term: 3, Data: []byte("c2")}}); err != nil {
		t.Fatal(err)
	}

	hs, entries, err = s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != (raft.HardState{Term: 4}) {
		t.Fatalf("hard state = %+v, want term 4 and no vote", hs)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[0].Index != 1 || string(entries[0].Data) != "a" {
		t.Fatalf("entry 1 = %+v", entries[0])
	}
	if entries[1].Index != 2 || len(entries[1].Data) != 0 {
		t.Fatalf("entry 2 = %+v, want an empty (no-op) entry", entries[1])
	}
	if entries[2].Index != 3 || entries[2].Term != 3 || string(entries[2].Data) != "c2" {
		t.Fatalf("entry 3 = %+v, want the entry that replaced the truncated one", entries[2])
	}

	if err := s.TruncateFrom(1); err != nil {
		t.Fatal(err)
	}
	if _, entries, _ = s.Load(); len(entries) != 0 {
		t.Fatalf("after TruncateFrom(1) got %d entries, want 0", len(entries))
	}
}

func TestMemStorageContract(t *testing.T) {
	storageContract(t, raft.NewMemStorage())
}

func TestWALStorageContract(t *testing.T) {
	s, err := raft.OpenWALStorage(filepath.Join(t.TempDir(), "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	storageContract(t, s)
}

func TestMemStorageRejectsGapsAndInjectsErrors(t *testing.T) {
	s := raft.NewMemStorage()
	if err := s.Append([]raft.Entry{{Index: 2, Term: 1}}); !errors.Is(err, raft.ErrOutOfOrder) {
		t.Fatalf("Append with a gap = %v, want ErrOutOfOrder", err)
	}
	if err := s.TruncateFrom(0); err == nil {
		t.Fatal("TruncateFrom(0) should fail")
	}

	boom := errors.New("boom")
	s.SetError(boom)
	if err := s.SaveHardState(raft.HardState{Term: 1}); !errors.Is(err, boom) {
		t.Fatalf("SaveHardState = %v, want boom", err)
	}
	if err := s.Append([]raft.Entry{{Index: 1, Term: 1}}); !errors.Is(err, boom) {
		t.Fatalf("Append = %v, want boom", err)
	}
	if err := s.TruncateFrom(1); !errors.Is(err, boom) {
		t.Fatalf("TruncateFrom = %v, want boom", err)
	}
	s.SetError(nil)
	if err := s.Append([]raft.Entry{{Index: 1, Term: 1}}); err != nil {
		t.Fatalf("Append after clearing the error: %v", err)
	}
}

func TestWALStoragePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")

	s, err := raft.OpenWALStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveHardState(raft.HardState{Term: 7, VotedFor: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateFrom(3); err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]raft.Entry{{Index: 3, Term: 4, Data: []byte("d")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := raft.OpenWALStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	hs, entries, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != (raft.HardState{Term: 7, VotedFor: "x"}) {
		t.Fatalf("hard state = %+v", hs)
	}
	if len(entries) != 3 || entries[2].Term != 4 || string(entries[2].Data) != "d" {
		t.Fatalf("entries after reopen = %+v", entries)
	}
}

func TestWALStorageDetectsGapAndNodeRefusesToStart(t *testing.T) {
	s, err := raft.OpenWALStorage(filepath.Join(t.TempDir(), "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// WALStorage does not validate appends; the gap must be caught on load.
	if err := s.Append([]raft.Entry{{Index: 2, Term: 1, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Load(); !errors.Is(err, raft.ErrCorruptStorage) {
		t.Fatalf("Load = %v, want ErrCorruptStorage", err)
	}
	_, err = raft.NewNode(raft.Config{ID: "a", Transport: nopTransport{}, Storage: s})
	if !errors.Is(err, raft.ErrCorruptStorage) {
		t.Fatalf("NewNode = %v, want ErrCorruptStorage", err)
	}
}
