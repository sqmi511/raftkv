package raft_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sqmi511/raftkv/internal/raft"
)

func termEntries(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func openWAL(t *testing.T, path string) *raft.WALStorage {
	t.Helper()
	s, err := raft.OpenWALStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// snapshotContract checks snapshot behaviour every Storage must share.
func snapshotContract(t *testing.T, s raft.Storage) {
	t.Helper()

	if snap, err := s.LoadSnapshot(); err != nil || snap.Index != 0 {
		t.Fatalf("fresh LoadSnapshot = %+v, %v; want empty", snap, err)
	}
	if err := s.Append(termEntries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}

	// A snapshot whose (index, term) matches the log keeps the entries after it.
	if err := s.SaveSnapshot(raft.Snapshot{Index: 6, Term: 1, Data: []byte("s6")}); err != nil {
		t.Fatal(err)
	}
	snap, err := s.LoadSnapshot()
	if err != nil || snap.Index != 6 || snap.Term != 1 || string(snap.Data) != "s6" {
		t.Fatalf("LoadSnapshot = %+v, %v", snap, err)
	}
	_, entries, err := s.Load()
	if err != nil || len(entries) != 4 || entries[0].Index != 7 || entries[3].Index != 10 {
		t.Fatalf("Load after snapshot = %+v, %v; want entries 7..10", entries, err)
	}

	// Appending continues after the log's end; truncation works above the snapshot.
	if err := s.Append(termEntries(11, 11, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateFrom(11); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(termEntries(11, 11, 2)); err != nil {
		t.Fatal(err)
	}
	if _, entries, _ = s.Load(); len(entries) != 5 || entries[4].Term != 2 {
		t.Fatalf("after truncate+append got %+v, want 5 entries ending with term 2", entries)
	}

	// A snapshot that is not newer is refused.
	if err := s.SaveSnapshot(raft.Snapshot{Index: 6, Term: 1}); err == nil {
		t.Fatal("re-saving the same snapshot index should fail")
	}
	if err := s.SaveSnapshot(raft.Snapshot{Index: 5, Term: 1}); err == nil {
		t.Fatal("an older snapshot should be refused")
	}

	// A snapshot that contradicts the log (entry 9 has term 1, not 5) replaces it entirely.
	if err := s.SaveSnapshot(raft.Snapshot{Index: 9, Term: 5, Data: []byte("s9")}); err != nil {
		t.Fatal(err)
	}
	if _, entries, _ = s.Load(); len(entries) != 0 {
		t.Fatalf("after a conflicting snapshot the log has %d entries, want 0", len(entries))
	}
	if err := s.Append(termEntries(10, 10, 5)); err != nil {
		t.Fatal(err)
	}
	if _, entries, _ = s.Load(); len(entries) != 1 || entries[0].Index != 10 {
		t.Fatalf("Load = %+v, want one entry with index 10", entries)
	}
}

func TestMemStorageSnapshotContract(t *testing.T) {
	snapshotContract(t, raft.NewMemStorage())
}

func TestWALStorageSnapshotContract(t *testing.T) {
	snapshotContract(t, openWAL(t, filepath.Join(t.TempDir(), "raft.wal")))
}

func TestWALStorageSnapshotPersistsAndShrinksLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	s := openWAL(t, path)

	if err := s.SaveHardState(raft.HardState{Term: 3, VotedFor: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(termEntries(1, 200, 1)); err != nil {
		t.Fatal(err)
	}
	before := fileSize(t, path)

	if err := s.SaveSnapshot(raft.Snapshot{Index: 190, Term: 1, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	if after := fileSize(t, path); after >= before/4 {
		t.Fatalf("WAL is %d bytes after compaction (was %d): it did not shrink", after, before)
	}

	// The compacted WAL is still appendable.
	if err := s.Append(termEntries(201, 201, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := openWAL(t, path)
	hs, entries, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != (raft.HardState{Term: 3, VotedFor: "x"}) {
		t.Fatalf("hard state = %+v: it must survive compaction", hs)
	}
	if len(entries) != 11 || entries[0].Index != 191 || entries[10].Index != 201 {
		t.Fatalf("entries after reopen = %d (first %d), want 11 starting at 191", len(entries), entries[0].Index)
	}
	snap, err := s2.LoadSnapshot()
	if err != nil || snap.Index != 190 || string(snap.Data) != "state" {
		t.Fatalf("LoadSnapshot = %+v, %v", snap, err)
	}
}

// A crash can land between writing the snapshot file and rewriting the WAL.
// Simulate it by restoring the OLD WAL next to the NEW snapshot file.
func TestWALStorageRecoversFromCrashDuringCompaction(t *testing.T) {
	cases := []struct {
		name      string
		snap      raft.Snapshot
		wantLen   int
		wantFirst uint64
	}{
		{"matching term keeps the suffix", raft.Snapshot{Index: 6, Term: 1, Data: []byte("s")}, 4, 7},
		{"conflicting term discards the log", raft.Snapshot{Index: 6, Term: 2, Data: []byte("s")}, 0, 0},
		{"snapshot ahead of the whole log", raft.Snapshot{Index: 15, Term: 1, Data: []byte("s")}, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "raft.wal")
			s := openWAL(t, path)
			if err := s.SaveHardState(raft.HardState{Term: 1}); err != nil {
				t.Fatal(err)
			}
			if err := s.Append(termEntries(1, 10, 1)); err != nil {
				t.Fatal(err)
			}
			oldWAL, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SaveSnapshot(tc.snap); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, oldWAL, 0o600); err != nil { // "crash" before the rewrite
				t.Fatal(err)
			}

			s2 := openWAL(t, path)
			snap, err := s2.LoadSnapshot()
			if err != nil || snap.Index != tc.snap.Index {
				t.Fatalf("LoadSnapshot = %+v, %v; want index %d", snap, err, tc.snap.Index)
			}
			hs, entries, err := s2.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if hs.Term != 1 {
				t.Fatalf("hard state = %+v, want term 1", hs)
			}
			if len(entries) != tc.wantLen || (tc.wantLen > 0 && entries[0].Index != tc.wantFirst) {
				t.Fatalf("Load = %d entries (first %v), want %d starting at %d",
					len(entries), entries, tc.wantLen, tc.wantFirst)
			}
		})
	}
}

func TestWALStorageDetectsCorruptSnapshotFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	s := openWAL(t, path)
	if err := s.Append(termEntries(1, 3, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot(raft.Snapshot{Index: 2, Term: 1, Data: []byte("abc")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	snapPath := path + ".snap"
	b, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(snapPath, b, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := raft.OpenWALStorage(path); !errors.Is(err, raft.ErrCorruptStorage) {
		t.Fatalf("OpenWALStorage = %v, want ErrCorruptStorage (the log before the snapshot is gone)", err)
	}
}
