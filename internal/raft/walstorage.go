package raft

import (
	"encoding/binary"
	"fmt"

	"github.com/sqmi511/raftkv/internal/wal"
)

// Record kinds stored in the WAL. Payload layouts (big endian):
//
//	hard state: kind | term (8B) | votedFor (rest)
//	entry:      kind | index (8B) | term (8B) | data (rest)
//	truncate:   kind | index (8B)   (drop every entry with index >= this)
//
// Loading replays the records in order, so later records override earlier ones.
const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3
)

// WALStorage is a durable Storage built on the write-ahead log. Every
// operation is fsynced before it returns.
//
// The log is append-only and is not compacted yet (snapshots come later).
type WALStorage struct {
	log *wal.WAL
}

// OpenWALStorage opens (or creates) the log file at path. Torn writes at the
// tail of the file are repaired by the WAL; corruption in the middle of the
// file is reported as an error.
func OpenWALStorage(path string) (*WALStorage, error) {
	w, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	return &WALStorage{log: w}, nil
}

// Load implements Storage by replaying the whole WAL.
func (s *WALStorage) Load() (HardState, []Entry, error) {
	var hs HardState
	var entries []Entry

	corrupt := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrCorruptStorage, fmt.Sprintf(format, args...))
	}

	err := s.log.Replay(func(p []byte) error {
		if len(p) == 0 {
			return corrupt("empty record")
		}
		switch p[0] {
		case recHardState:
			if len(p) < 9 {
				return corrupt("short hard-state record (%d bytes)", len(p))
			}
			hs = HardState{Term: binary.BigEndian.Uint64(p[1:9]), VotedFor: string(p[9:])}

		case recEntry:
			if len(p) < 17 {
				return corrupt("short entry record (%d bytes)", len(p))
			}
			e := Entry{
				Index: binary.BigEndian.Uint64(p[1:9]),
				Term:  binary.BigEndian.Uint64(p[9:17]),
			}
			if want := uint64(len(entries)) + 1; e.Index != want {
				return corrupt("entry index %d, want %d", e.Index, want)
			}
			if len(p) > 17 {
				e.Data = p[17:] // Replay hands us a fresh slice, so keeping it is safe
			}
			entries = append(entries, e)

		case recTruncate:
			if len(p) != 9 {
				return corrupt("bad truncate record (%d bytes)", len(p))
			}
			idx := binary.BigEndian.Uint64(p[1:9])
			if idx < 1 || idx > uint64(len(entries))+1 {
				return corrupt("truncate index %d with %d entries", idx, len(entries))
			}
			entries = entries[:idx-1]

		default:
			return corrupt("unknown record kind %d", p[0])
		}
		return nil
	})
	if err != nil {
		return HardState{}, nil, err
	}
	return hs, entries, nil
}

// SaveHardState implements Storage.
func (s *WALStorage) SaveHardState(hs HardState) error {
	rec := make([]byte, 0, 9+len(hs.VotedFor))
	rec = append(rec, recHardState)
	rec = binary.BigEndian.AppendUint64(rec, hs.Term)
	rec = append(rec, hs.VotedFor...)
	return s.log.Append(rec)
}

// Append implements Storage: one write and one fsync for the whole batch.
func (s *WALStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	recs := make([][]byte, 0, len(entries))
	for _, e := range entries {
		rec := make([]byte, 0, 17+len(e.Data))
		rec = append(rec, recEntry)
		rec = binary.BigEndian.AppendUint64(rec, e.Index)
		rec = binary.BigEndian.AppendUint64(rec, e.Term)
		rec = append(rec, e.Data...)
		recs = append(recs, rec)
	}
	return s.log.AppendBatch(recs)
}

// TruncateFrom implements Storage.
func (s *WALStorage) TruncateFrom(index uint64) error {
	rec := make([]byte, 0, 9)
	rec = append(rec, recTruncate)
	rec = binary.BigEndian.AppendUint64(rec, index)
	return s.log.Append(rec)
}

// Close implements Storage.
func (s *WALStorage) Close() error { return s.log.Close() }
