package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---- helpers ---------------------------------------------------------------

func testRecords(n int) [][]byte {
	recs := make([][]byte, n)
	for i := range recs {
		recs[i] = []byte(fmt.Sprintf("record-%d-%s", i, strings.Repeat("x", i*3)))
	}
	return recs
}

// offsetOf returns the file offset where record i starts.
func offsetOf(recs [][]byte, i int) int64 {
	var off int64
	for _, r := range recs[:i] {
		off += int64(headerSize + len(r))
	}
	return off
}

func mustOpen(t *testing.T, path string, opts ...Option) *WAL {
	t.Helper()
	w, err := Open(path, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func readAll(t *testing.T, w *WAL) [][]byte {
	t.Helper()
	var out [][]byte
	err := w.Replay(func(p []byte) error {
		out = append(out, p)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return out
}

func requireRecords(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// writeLog creates a log file containing recs and closes it.
func writeLog(t *testing.T, path string, recs [][]byte) {
	t.Helper()
	w, err := Open(path, WithSyncOnAppend(false))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.AppendBatch(recs); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func appendRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func patchFile(t *testing.T, path string, off int64, mutate func(b []byte)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutate(data[off:])
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	patchFile(t, path, off, func(b []byte) { b[0] ^= 0xFF })
}

func tempLog(t *testing.T) string {
	return filepath.Join(t.TempDir(), "test.wal")
}

// ---- basic behaviour -------------------------------------------------------

func TestAppendReplayAndReopen(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(10)

	w := mustOpen(t, path)
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	requireRecords(t, readAll(t, w), recs)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := mustOpen(t, path)
	requireRecords(t, readAll(t, w2), recs)
	if got := w2.Recovery(); got.Records != 10 || got.TruncatedBytes != 0 {
		t.Fatalf("Recovery = %+v, want 10 records, 0 truncated", got)
	}

	// appending after reopen continues the log
	extra := []byte("after-reopen")
	if err := w2.Append(extra); err != nil {
		t.Fatal(err)
	}
	requireRecords(t, readAll(t, w2), append(append([][]byte{}, recs...), extra))
}

func TestNewLogIsEmpty(t *testing.T) {
	w := mustOpen(t, tempLog(t))
	if got := readAll(t, w); len(got) != 0 {
		t.Fatalf("new log has %d records", len(got))
	}
	if w.Size() != 0 {
		t.Fatalf("Size = %d, want 0", w.Size())
	}
}

func TestEmptyPayload(t *testing.T) {
	path := tempLog(t)
	w := mustOpen(t, path)
	if err := w.Append(nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte{}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	w2 := mustOpen(t, path)
	got := readAll(t, w2)
	if len(got) != 2 || len(got[0]) != 0 || len(got[1]) != 0 {
		t.Fatalf("got %v, want two empty records", got)
	}
}

func TestAppendBatch(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)

	w := mustOpen(t, path)
	if err := w.AppendBatch(nil); err != nil { // empty batch is a no-op
		t.Fatal(err)
	}
	if err := w.AppendBatch(recs); err != nil {
		t.Fatal(err)
	}
	if want := offsetOf(recs, len(recs)); w.Size() != want {
		t.Fatalf("Size = %d, want %d", w.Size(), want)
	}
	requireRecords(t, readAll(t, w), recs)
}

func TestRecordTooLarge(t *testing.T) {
	w := mustOpen(t, tempLog(t), WithMaxRecordSize(10))

	if err := w.Append(make([]byte, 11)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err = %v, want ErrRecordTooLarge", err)
	}
	// a batch containing one oversized record writes nothing at all
	err := w.AppendBatch([][]byte{[]byte("ok"), make([]byte, 11)})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err = %v, want ErrRecordTooLarge", err)
	}
	if w.Size() != 0 {
		t.Fatalf("Size = %d, want 0", w.Size())
	}
	if err := w.Append(make([]byte, 10)); err != nil { // exactly at the limit
		t.Fatalf("Append at limit: %v", err)
	}
}

func TestClosedWAL(t *testing.T) {
	w := mustOpen(t, tempLog(t))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if err := w.Append([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync = %v, want ErrClosed", err)
	}
	if err := w.Replay(func([]byte) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replay = %v, want ErrClosed", err)
	}
}

func TestOpenBadPath(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "missing-dir", "x.wal"))
	if err == nil {
		t.Fatal("Open in a nonexistent directory should fail")
	}
}

func TestReplayStopsOnCallbackError(t *testing.T) {
	w := mustOpen(t, tempLog(t))
	if err := w.AppendBatch(testRecords(5)); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("stop")
	calls := 0
	err := w.Replay(func([]byte) error {
		calls++
		if calls == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || calls != 2 {
		t.Fatalf("err = %v, calls = %d; want sentinel after 2 calls", err, calls)
	}
}

func TestExplicitSync(t *testing.T) {
	w := mustOpen(t, tempLog(t), WithSyncOnAppend(false))
	if err := w.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestConcurrentAppend(t *testing.T) {
	path := tempLog(t)
	w := mustOpen(t, path, WithSyncOnAppend(false))

	const (
		goroutines = 8
		perG       = 50
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if err := w.Append([]byte(fmt.Sprintf("g%d-%d", g, i))); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	for _, p := range readAll(t, w) {
		if seen[string(p)] {
			t.Fatalf("duplicate record %q", p)
		}
		seen[string(p)] = true
	}
	if len(seen) != goroutines*perG {
		t.Fatalf("got %d distinct records, want %d", len(seen), goroutines*perG)
	}
}

// ---- crash simulation ------------------------------------------------------

// A crash can cut the final record at ANY byte. For every possible cut point
// inside the last record, recovery must keep exactly the complete records,
// discard the partial one, and leave the log appendable.
func TestTornWriteAtEveryOffset(t *testing.T) {
	recs := testRecords(5)
	src := tempLog(t)
	writeLog(t, src, recs)

	full, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	lastStart := offsetOf(recs, 4)

	for cut := lastStart; cut < int64(len(full)); cut++ {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("cut-%d.wal", cut))
		if err := os.WriteFile(path, full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}

		w, err := Open(path)
		if err != nil {
			t.Fatalf("cut=%d: Open: %v", cut, err)
		}
		requireRecords(t, readAll(t, w), recs[:4])
		if got, want := w.Recovery().TruncatedBytes, cut-lastStart; got != want {
			t.Fatalf("cut=%d: TruncatedBytes = %d, want %d", cut, got, want)
		}

		// The log must accept new writes right after recovery.
		if err := w.Append(recs[4]); err != nil {
			t.Fatalf("cut=%d: Append after recovery: %v", cut, err)
		}
		_ = w.Close()

		w2, err := Open(path)
		if err != nil {
			t.Fatalf("cut=%d: reopen: %v", cut, err)
		}
		requireRecords(t, readAll(t, w2), recs)
		_ = w2.Close()
	}
}

// Real process crash: a child process appends records, writes half a record
// through a raw file handle, then dies via os.Exit without Close.
const crashEnv = "RAFTKV_WAL_CRASH_PATH"

func TestCrashChild(t *testing.T) {
	path := os.Getenv(crashEnv)
	if path == "" {
		t.Skip("helper process; it is started by TestCrashRecovery")
	}
	w, err := Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	for _, r := range testRecords(100) {
		if err := w.Append(r); err != nil {
			fmt.Fprintln(os.Stderr, "append:", err)
			os.Exit(2)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		os.Exit(2)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		os.Exit(2)
	}
	os.Exit(3) // crash: no Close, no deferred cleanup
}

func TestCrashRecovery(t *testing.T) {
	path := tempLog(t)

	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(), crashEnv+"="+path)
	out, err := cmd.CombinedOutput()

	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Fatalf("child should exit with code 3, got err=%v\n%s", err, out)
	}

	w := mustOpen(t, path)
	requireRecords(t, readAll(t, w), testRecords(100))
	if got := w.Recovery().TruncatedBytes; got != 5 {
		t.Fatalf("TruncatedBytes = %d, want 5", got)
	}
}

// ---- corruption handling ---------------------------------------------------

func TestPartialHeaderAndZeroTailAreTruncated(t *testing.T) {
	cases := []struct {
		name string
		tail []byte
	}{
		{"partial header", []byte{0xAB, 0xCD, 0xEF}},
		{"zero filled tail", make([]byte, 100)},
		{"zero filled tail larger than a read chunk", make([]byte, 40<<10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tempLog(t)
			recs := testRecords(5)
			writeLog(t, path, recs)
			appendRaw(t, path, tc.tail)

			w := mustOpen(t, path)
			requireRecords(t, readAll(t, w), recs)
			if got := w.Recovery().TruncatedBytes; got != int64(len(tc.tail)) {
				t.Fatalf("TruncatedBytes = %d, want %d", got, len(tc.tail))
			}
			if want := offsetOf(recs, 5); w.Size() != want {
				t.Fatalf("Size = %d, want %d", w.Size(), want)
			}
		})
	}
}

func TestCorruptLastRecordIsTruncated(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)

	// flip a payload byte of the final record: classic torn/garbled tail
	flipByte(t, path, offsetOf(recs, 4)+headerSize)

	w := mustOpen(t, path)
	requireRecords(t, readAll(t, w), recs[:4])
	if got, want := w.Recovery().TruncatedBytes, int64(headerSize+len(recs[4])); got != want {
		t.Fatalf("TruncatedBytes = %d, want %d", got, want)
	}
}

func TestMidLogCorruptionIsReported(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)

	flipByte(t, path, offsetOf(recs, 1)+headerSize) // damage record #1 payload

	_, err := Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
	var ce *CorruptError
	if !errors.As(err, &ce) || ce.Offset != offsetOf(recs, 1) {
		t.Fatalf("CorruptError = %+v, want offset %d", ce, offsetOf(recs, 1))
	}

	// and the file must NOT have been modified by the failed open
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != offsetOf(recs, 5) {
		t.Fatalf("file size changed to %d after failed Open", fi.Size())
	}
}

func TestCorruptChecksumFieldIsReported(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)

	flipByte(t, path, offsetOf(recs, 2)) // first byte of record #2's CRC

	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestCorruptLengthIsReported(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)

	patchFile(t, path, offsetOf(recs, 1)+4, func(b []byte) { // record #1's length
		copy(b, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	})

	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}
}

func TestGarbageTailIsReportedUnlessRepairing(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)
	appendRaw(t, path, bytes.Repeat([]byte{0xFF}, 20))

	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt", err)
	}

	w := mustOpen(t, path, WithRepair(true))
	requireRecords(t, readAll(t, w), recs)
	if got := w.Recovery().TruncatedBytes; got != 20 {
		t.Fatalf("TruncatedBytes = %d, want 20", got)
	}
}

func TestRepairTruncatesAtFirstCorruptRecord(t *testing.T) {
	path := tempLog(t)
	recs := testRecords(5)
	writeLog(t, path, recs)
	flipByte(t, path, offsetOf(recs, 2)+headerSize)

	w := mustOpen(t, path, WithRepair(true))
	requireRecords(t, readAll(t, w), recs[:2]) // records 2..4 are sacrificed
	if got, want := w.Recovery().TruncatedBytes, offsetOf(recs, 5)-offsetOf(recs, 2); got != want {
		t.Fatalf("TruncatedBytes = %d, want %d", got, want)
	}

	// repaired log is healthy: appends work and a plain Open succeeds
	if err := w.Append([]byte("fresh")); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w2 := mustOpen(t, path)
	if got := readAll(t, w2); len(got) != 3 || string(got[2]) != "fresh" {
		t.Fatalf("after repair got %d records, want 3 ending in \"fresh\"", len(got))
	}
}
