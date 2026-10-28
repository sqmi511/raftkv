// Package wal implements a checksummed, fsync'd, append-only write-ahead log.
//
// On-disk record format (little endian):
//
//	+----------------+----------------+-----------------+
//	| crc32c (4B)    | length (4B)    | payload (length)|
//	+----------------+----------------+-----------------+
//
// The checksum (CRC-32C) covers the length field and the payload.
//
// Crash recovery happens in Open: the file is scanned from the start and
//   - an incomplete or corrupt record at the very end of the file (a torn
//     write), or a zero-filled tail, is truncated away silently;
//   - corruption followed by more data (damage in the middle of the log) is
//     reported as ErrCorrupt, unless WithRepair(true) is set, in which case
//     the log is truncated at the first bad record.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	headerSize = 8

	// DefaultMaxRecordSize is the largest accepted payload (64 MiB). It also
	// protects recovery from huge allocations caused by a corrupt length.
	DefaultMaxRecordSize = 64 << 20
)

var (
	// ErrCorrupt is matched (via errors.Is) by *CorruptError.
	ErrCorrupt = errors.New("wal: corrupt record")
	// ErrClosed is returned for operations on a closed WAL.
	ErrClosed = errors.New("wal: closed")
	// ErrRecordTooLarge is returned when a payload exceeds the size limit.
	ErrRecordTooLarge = errors.New("wal: record too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CorruptError describes corruption found in the middle of the log.
type CorruptError struct {
	Offset int64  // byte offset of the first bad record
	Reason string // human-readable cause
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal: corrupt record at offset %d: %s", e.Offset, e.Reason)
}

// Is makes errors.Is(err, ErrCorrupt) work.
func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }

// Recovery summarises what Open found on disk.
type Recovery struct {
	Records        int   // valid records kept
	ValidBytes     int64 // size of the valid prefix
	TruncatedBytes int64 // bytes discarded from the tail
}

// Option configures a WAL.
type Option func(*WAL)

// WithSyncOnAppend controls whether every Append/AppendBatch fsyncs before
// returning. Default: true. If disabled, call Sync yourself.
func WithSyncOnAppend(b bool) Option { return func(w *WAL) { w.syncOnAppend = b } }

// WithMaxRecordSize sets the maximum payload size.
func WithMaxRecordSize(n int) Option { return func(w *WAL) { w.maxRec = n } }

// WithRepair makes Open truncate the log at the first corrupt record even if
// valid-looking data follows it. Data after that point is lost.
func WithRepair(b bool) Option { return func(w *WAL) { w.repair = b } }

// WAL is an append-only log file. It is safe for concurrent use.
type WAL struct {
	mu           sync.Mutex
	f            *os.File
	path         string
	size         int64 // end of valid data; next record is written here
	closed       bool
	err          error // sticky failure; the WAL is unusable once set
	rec          Recovery
	syncOnAppend bool
	maxRec       int
	repair       bool
}

// Open opens (or creates) the log at path, recovers from any crash damage
// and positions the log for appending.
func Open(path string, opts ...Option) (*WAL, error) {
	w := &WAL{path: path, syncOnAppend: true, maxRec: DefaultMaxRecordSize}
	for _, opt := range opts {
		opt(w)
	}

	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	fail := func(err error) (*WAL, error) {
		_ = f.Close()
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return fail(fmt.Errorf("wal: stat: %w", err))
	}

	res, err := scan(f, fi.Size(), w.maxRec, nil)
	if err != nil {
		return fail(fmt.Errorf("wal: recovery scan: %w", err))
	}
	if res.corrupt != nil && !res.tail && !w.repair {
		return fail(res.corrupt)
	}

	w.rec = Recovery{
		Records:        res.count,
		ValidBytes:     res.end,
		TruncatedBytes: fi.Size() - res.end,
	}
	if res.end < fi.Size() {
		if err := f.Truncate(res.end); err != nil {
			return fail(fmt.Errorf("wal: truncate torn tail: %w", err))
		}
		if err := f.Sync(); err != nil {
			return fail(fmt.Errorf("wal: sync after truncate: %w", err))
		}
	}
	if created {
		syncDir(filepath.Dir(path))
	}

	w.f = f
	w.size = res.end
	return w, nil
}

// Recovery reports what Open recovered.
func (w *WAL) Recovery() Recovery { return w.rec }

// Size returns the size in bytes of the valid log.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Append writes one record.
func (w *WAL) Append(payload []byte) error {
	return w.AppendBatch([][]byte{payload})
}

// AppendBatch writes several records with a single write and (if enabled) a
// single fsync. It is all-or-nothing from the caller's point of view: on error
// the log is rolled back, or becomes permanently failed if that is impossible.
func (w *WAL) AppendBatch(payloads [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.usable(); err != nil {
		return err
	}
	if len(payloads) == 0 {
		return nil
	}

	total := 0
	for _, p := range payloads {
		if len(p) > w.maxRec {
			return fmt.Errorf("%w: %d bytes (max %d)", ErrRecordTooLarge, len(p), w.maxRec)
		}
		total += headerSize + len(p)
	}
	buf := make([]byte, 0, total)
	for _, p := range payloads {
		buf = appendRecord(buf, p)
	}

	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		return w.rollback(err)
	}
	if w.syncOnAppend {
		if err := w.f.Sync(); err != nil {
			// After a failed fsync the kernel may have dropped dirty pages,
			// so retrying is unsafe. Fail permanently; reopen to recover.
			w.err = fmt.Errorf("wal: fsync failed: %w", err)
			return w.err
		}
	}
	w.size += int64(len(buf))
	return nil
}

// Sync flushes the log to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.usable(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: fsync failed: %w", err)
		return w.err
	}
	return nil
}

// Replay calls fn for every record in order. Each payload is a fresh slice
// that fn may keep. fn must not call methods on the same WAL (the WAL's lock
// is held). Replay stops and returns the first error from fn.
func (w *WAL) Replay(fn func(payload []byte) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	res, err := scan(w.f, w.size, w.maxRec, fn)
	if err != nil {
		return err
	}
	if res.corrupt != nil {
		return res.corrupt // the file changed underneath us
	}
	if res.end != w.size {
		return &CorruptError{Offset: res.end, Reason: "log shorter than expected"}
	}
	return nil
}

// Close syncs and closes the log. It is safe to call more than once.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	var syncErr error
	if w.err == nil {
		syncErr = w.f.Sync()
	}
	return errors.Join(syncErr, w.f.Close())
}

func (w *WAL) usable() error {
	if w.closed {
		return ErrClosed
	}
	return w.err
}

// rollback removes a partially written batch so later appends don't land
// after garbage.
func (w *WAL) rollback(cause error) error {
	if err := w.f.Truncate(w.size); err != nil {
		w.err = fmt.Errorf("wal: write failed (%v) and rollback failed: %w", cause, err)
		return w.err
	}
	return fmt.Errorf("wal: write: %w", cause)
}

func appendRecord(dst, payload []byte) []byte {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[0:4], checksum(hdr[4:8], payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

func checksum(lenBytes, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, lenBytes)
	return crc32.Update(c, castagnoli, payload)
}

type scanResult struct {
	end     int64         // offset just past the last valid record
	count   int           // number of valid records
	corrupt *CorruptError // first corrupt record, if any
	tail    bool          // true if the damage looks like a torn write at EOF
}

// scan walks the log from offset 0, calling fn (if non-nil) for each valid
// record, and stops at the first incomplete or corrupt one.
func scan(r io.ReaderAt, size int64, maxRec int, fn func([]byte) error) (scanResult, error) {
	var res scanResult
	var hdr [headerSize]byte
	off := int64(0)

	for off < size {
		if size-off < headerSize { // partial header: torn write
			res.tail = true
			break
		}
		if err := readFull(r, hdr[:], off); err != nil {
			return res, err
		}
		sum := binary.LittleEndian.Uint32(hdr[0:4])
		length := binary.LittleEndian.Uint32(hdr[4:8])

		if int64(length) > int64(maxRec) {
			zero, err := allZero(r, off, size)
			if err != nil {
				return res, err
			}
			res.tail = zero
			res.corrupt = &CorruptError{Offset: off, Reason: fmt.Sprintf("record length %d exceeds limit %d", length, maxRec)}
			break
		}

		end := off + headerSize + int64(length)
		if end > size { // partial payload: torn write
			res.tail = true
			break
		}

		payload := make([]byte, length)
		if err := readFull(r, payload, off+headerSize); err != nil {
			return res, err
		}
		if checksum(hdr[4:8], payload) != sum {
			zero, err := allZero(r, off, size)
			if err != nil {
				return res, err
			}
			res.tail = end == size || zero // last record, or zero-filled tail
			res.corrupt = &CorruptError{Offset: off, Reason: "checksum mismatch"}
			break
		}

		if fn != nil {
			if err := fn(payload); err != nil {
				return res, err
			}
		}
		off = end
		res.count++
	}

	res.end = off
	return res, nil
}

// readFull reads exactly len(buf) bytes at off.
func readFull(r io.ReaderAt, buf []byte, off int64) error {
	n, err := r.ReadAt(buf, off)
	if n == len(buf) {
		return nil
	}
	if err == nil || errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// allZero reports whether every byte in [off, size) is zero.
func allZero(r io.ReaderAt, off, size int64) (bool, error) {
	buf := make([]byte, 32<<10)
	for off < size {
		n := int64(len(buf))
		if size-off < n {
			n = size - off
		}
		if err := readFull(r, buf[:n], off); err != nil {
			return false, err
		}
		for _, b := range buf[:n] {
			if b != 0 {
				return false, nil
			}
		}
		off += n
	}
	return true, nil
}

// syncDir fsyncs a directory so a newly created file survives a crash.
// Best effort: unsupported on Windows, where the error is ignored.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
