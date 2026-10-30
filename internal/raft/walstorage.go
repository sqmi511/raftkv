package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/sqmi511/raftkv/internal/wal"
)

// Record kinds stored in the WAL. Payload layouts (big endian):
//
//	hard state: kind | term (8B) | votedFor (rest)
//	entry:      kind | index (8B) | term (8B) | data (rest)
//	truncate:   kind | index (8B)   (drop every entry with index >= this)
//
// Loading replays the records in order, so later records override earlier ones.
// After a compaction the WAL starts at the first entry after the snapshot, so
// the first entry record may have any index.
const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3
)

// File names next to the WAL file.
const (
	snapSuffix    = ".snap"     // the snapshot: <wal>.snap
	tmpSuffix     = ".tmp"      // temporary files used for atomic replacement
	snapFileMagic = "RKSN"      // first four bytes of a snapshot file
	snapHeaderLen = 4 + 4 + 8*2 // magic, crc, index, term
)

var snapCRC = crc32.MakeTable(crc32.Castagnoli)

// WALStorage is a durable Storage built on the write-ahead log. Every
// operation is fsynced before it returns.
//
// The snapshot lives in "<path>.snap", replaced atomically (temp file, fsync,
// rename). Compaction then rewrites the WAL without the covered entries, again
// via a temp file and rename. A crash between the two steps is harmless:
// Load ignores entries the snapshot covers, and discards the whole log if it
// disagrees with the snapshot.
type WALStorage struct {
	path string
	log  *wal.WAL
	snap Snapshot
	err  error // sticky failure after a half-finished compaction
}

// OpenWALStorage opens (or creates) the log file at path and its snapshot.
// Torn writes at the tail of the log are repaired by the WAL; corruption in
// the middle of the log or in the snapshot file is reported as an error.
func OpenWALStorage(path string) (*WALStorage, error) {
	snap, err := readSnapshotFile(path + snapSuffix)
	if err != nil {
		return nil, err
	}
	// Leftovers from a compaction that crashed before its rename.
	_ = os.Remove(path + tmpSuffix)
	_ = os.Remove(path + snapSuffix + tmpSuffix)

	w, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	return &WALStorage{path: path, log: w, snap: snap}, nil
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptStorage, fmt.Sprintf(format, args...))
}

// replay reads the raw hard state and all entries in the WAL (before any
// snapshot filtering).
func (s *WALStorage) replay() (HardState, []Entry, error) {
	var hs HardState
	var entries []Entry
	var next uint64 // index the next entry record must have; 0 = any

	err := s.log.Replay(func(p []byte) error {
		if len(p) == 0 {
			return corruptf("empty record")
		}
		switch p[0] {
		case recHardState:
			if len(p) < 9 {
				return corruptf("short hard-state record (%d bytes)", len(p))
			}
			hs = HardState{Term: binary.BigEndian.Uint64(p[1:9]), VotedFor: string(p[9:])}

		case recEntry:
			if len(p) < 17 {
				return corruptf("short entry record (%d bytes)", len(p))
			}
			e := Entry{
				Index: binary.BigEndian.Uint64(p[1:9]),
				Term:  binary.BigEndian.Uint64(p[9:17]),
			}
			if e.Index == 0 {
				return corruptf("entry with index 0")
			}
			if next != 0 && e.Index != next {
				return corruptf("entry index %d, want %d", e.Index, next)
			}
			if len(p) > 17 {
				e.Data = p[17:] // Replay hands us a fresh slice, so keeping it is safe
			}
			entries = append(entries, e)
			next = e.Index + 1

		case recTruncate:
			if len(p) != 9 {
				return corruptf("bad truncate record (%d bytes)", len(p))
			}
			idx := binary.BigEndian.Uint64(p[1:9])
			if idx < 1 {
				return corruptf("truncate index 0")
			}
			if len(entries) == 0 {
				return nil // nothing to remove
			}
			base := entries[0].Index
			if idx < base || idx > next {
				return corruptf("truncate index %d with entries %d..%d", idx, base, next-1)
			}
			entries = entries[:idx-base]
			next = idx

		default:
			return corruptf("unknown record kind %d", p[0])
		}
		return nil
	})
	if err != nil {
		return HardState{}, nil, err
	}
	return hs, entries, nil
}

// Load implements Storage: it replays the WAL and drops what the snapshot covers.
func (s *WALStorage) Load() (HardState, []Entry, error) {
	if s.err != nil {
		return HardState{}, nil, s.err
	}
	hs, all, err := s.replay()
	if err != nil {
		return HardState{}, nil, err
	}
	kept, err := suffixAfter(all, s.snap)
	if err != nil {
		return HardState{}, nil, err
	}
	return hs, kept, nil
}

// LoadSnapshot implements Storage.
func (s *WALStorage) LoadSnapshot() (Snapshot, error) { return s.snap, nil }

// SaveHardState implements Storage.
func (s *WALStorage) SaveHardState(hs HardState) error {
	if s.err != nil {
		return s.err
	}
	return s.log.Append(encodeHardState(hs))
}

// Append implements Storage: one write and one fsync for the whole batch.
func (s *WALStorage) Append(entries []Entry) error {
	if s.err != nil {
		return s.err
	}
	if len(entries) == 0 {
		return nil
	}
	recs := make([][]byte, 0, len(entries))
	for _, e := range entries {
		recs = append(recs, encodeEntry(e))
	}
	return s.log.AppendBatch(recs)
}

// TruncateFrom implements Storage.
func (s *WALStorage) TruncateFrom(index uint64) error {
	if s.err != nil {
		return s.err
	}
	rec := make([]byte, 0, 9)
	rec = append(rec, recTruncate)
	rec = binary.BigEndian.AppendUint64(rec, index)
	return s.log.Append(rec)
}

// SaveSnapshot implements Storage.
func (s *WALStorage) SaveSnapshot(snap Snapshot) error {
	if s.err != nil {
		return s.err
	}
	if snap.Index <= s.snap.Index {
		return fmt.Errorf("raft: storage: snapshot index %d is not newer than %d", snap.Index, s.snap.Index)
	}
	hs, all, err := s.replay()
	if err != nil {
		return err
	}
	kept, err := suffixAfter(all, snap)
	if err != nil {
		return err
	}

	// Step 1: the snapshot becomes durable. From here on, recovery is correct
	// even if we crash before step 2.
	if err := writeSnapshotFile(s.path+snapSuffix, snap); err != nil {
		return fmt.Errorf("raft: save snapshot: %w", err)
	}
	s.snap = Snapshot{Index: snap.Index, Term: snap.Term, Data: append([]byte(nil), snap.Data...)}

	// Step 2: drop the covered entries from the WAL.
	if err := s.rewrite(hs, kept); err != nil {
		s.err = fmt.Errorf("raft: compact log: %w", err)
		return s.err
	}
	return nil
}

// rewrite replaces the WAL with a fresh file holding hs and entries.
func (s *WALStorage) rewrite(hs HardState, entries []Entry) error {
	tmp := s.path + tmpSuffix
	_ = os.Remove(tmp)
	nw, err := wal.Open(tmp)
	if err != nil {
		return err
	}
	var recs [][]byte
	if hs != (HardState{}) {
		recs = append(recs, encodeHardState(hs))
	}
	for _, e := range entries {
		recs = append(recs, encodeEntry(e))
	}
	if err := nw.AppendBatch(recs); err != nil {
		_ = nw.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := nw.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// Windows cannot replace a file that is still open, so close first.
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	syncDir(filepath.Dir(s.path))

	w, err := wal.Open(s.path)
	if err != nil {
		return err
	}
	s.log = w
	return nil
}

// Close implements Storage.
func (s *WALStorage) Close() error { return s.log.Close() }

func encodeHardState(hs HardState) []byte {
	rec := make([]byte, 0, 9+len(hs.VotedFor))
	rec = append(rec, recHardState)
	rec = binary.BigEndian.AppendUint64(rec, hs.Term)
	return append(rec, hs.VotedFor...)
}

func encodeEntry(e Entry) []byte {
	rec := make([]byte, 0, 17+len(e.Data))
	rec = append(rec, recEntry)
	rec = binary.BigEndian.AppendUint64(rec, e.Index)
	rec = binary.BigEndian.AppendUint64(rec, e.Term)
	return append(rec, e.Data...)
}

// ---- snapshot file ----------------------------------------------------------

// Layout: "RKSN" | crc32c (4B, over everything after it) | index (8B) | term (8B) | data

func encodeSnapshotFile(s Snapshot) []byte {
	buf := make([]byte, 0, snapHeaderLen+len(s.Data))
	buf = append(buf, snapFileMagic...)
	buf = append(buf, 0, 0, 0, 0) // checksum placeholder
	buf = binary.BigEndian.AppendUint64(buf, s.Index)
	buf = binary.BigEndian.AppendUint64(buf, s.Term)
	buf = append(buf, s.Data...)
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(buf[8:], snapCRC))
	return buf
}

func decodeSnapshotFile(b []byte) (Snapshot, error) {
	if len(b) < snapHeaderLen || !bytes.Equal(b[:4], []byte(snapFileMagic)) {
		return Snapshot{}, corruptf("snapshot file has a bad header")
	}
	if crc32.Checksum(b[8:], snapCRC) != binary.BigEndian.Uint32(b[4:8]) {
		return Snapshot{}, corruptf("snapshot file checksum mismatch")
	}
	s := Snapshot{
		Index: binary.BigEndian.Uint64(b[8:16]),
		Term:  binary.BigEndian.Uint64(b[16:24]),
	}
	if s.Index == 0 {
		return Snapshot{}, corruptf("snapshot file has index 0")
	}
	if len(b) > snapHeaderLen {
		s.Data = b[snapHeaderLen:]
	}
	return s, nil
}

// readSnapshotFile returns the zero Snapshot if the file does not exist. A
// damaged file is an error: the log it replaced may already be gone.
func readSnapshotFile(path string) (Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("raft: read snapshot: %w", err)
	}
	return decodeSnapshotFile(b)
}

// writeSnapshotFile atomically replaces the file at path.
func writeSnapshotFile(path string, s Snapshot) error {
	tmp := path + tmpSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeSnapshotFile(s)); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir fsyncs a directory so renames survive a crash. Best effort:
// unsupported on Windows, where the error is ignored.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
