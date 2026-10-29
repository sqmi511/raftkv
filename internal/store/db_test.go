package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/wal"
)

func dbPath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "kv.wal")
}

func openTestDB(t *testing.T, path string, clk *fakeClock) *DB {
	t.Helper()
	db, err := OpenDB(path, WithClock(clk.Now), WithSweepInterval(0))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustSet(t *testing.T, db *DB, key, val string, ttl time.Duration) {
	t.Helper()
	if err := db.Set(key, []byte(val), ttl); err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
}

func mustHave(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, ok := db.Get(key)
	if !ok || string(got) != want {
		t.Fatalf("Get(%q) = %q, %v; want %q, true", key, got, ok, want)
	}
}

func mustMiss(t *testing.T, db *DB, key string) {
	t.Helper()
	if got, ok := db.Get(key); ok {
		t.Fatalf("Get(%q) = %q, true; want missing", key, got)
	}
}

func TestDBPersistsAcrossReopen(t *testing.T) {
	path := dbPath(t)
	clk := newFakeClock()

	db := openTestDB(t, path, clk)
	mustSet(t, db, "a", "1", 0)
	mustSet(t, db, "b", "2", 0)
	mustSet(t, db, "a", "3", 0) // overwrite
	mustSet(t, db, "c", "4", 0)
	if ok, err := db.Delete("b"); err != nil || !ok {
		t.Fatalf("Delete(b) = %v, %v; want true, nil", ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openTestDB(t, path, clk)
	mustHave(t, db2, "a", "3")
	mustMiss(t, db2, "b")
	mustHave(t, db2, "c", "4")
}

func TestDBTTLIsAbsoluteAcrossRestart(t *testing.T) {
	path := dbPath(t)
	clk := newFakeClock()

	db := openTestDB(t, path, clk)
	mustSet(t, db, "k", "v", 10*time.Second)
	clk.Advance(4 * time.Second)
	_ = db.Close()

	// 6s of lifetime remain; a restart must not grant a fresh 10s.
	db2 := openTestDB(t, path, clk)
	clk.Advance(5 * time.Second)
	mustHave(t, db2, "k", "v")
	clk.Advance(2 * time.Second)
	mustMiss(t, db2, "k")
}

func TestDBExpiredDuringDowntimeIsNotResurrected(t *testing.T) {
	path := dbPath(t)
	clk := newFakeClock()

	db := openTestDB(t, path, clk)
	mustSet(t, db, "k", "old", 0)
	mustSet(t, db, "k", "new", 5*time.Second) // supersedes "old", then expires
	_ = db.Close()

	clk.Advance(10 * time.Second) // server is down meanwhile
	db2 := openTestDB(t, path, clk)
	mustMiss(t, db2, "k") // must not fall back to "old"
}

func TestDBDeleteMissingWritesNothing(t *testing.T) {
	clk := newFakeClock()
	db := openTestDB(t, dbPath(t), clk)

	before := db.log.Size()
	ok, err := db.Delete("nope")
	if err != nil || ok {
		t.Fatalf("Delete(nope) = %v, %v; want false, nil", ok, err)
	}
	if db.log.Size() != before {
		t.Fatal("deleting a missing key must not append to the log")
	}
}

func TestDBRecoversFromTornTail(t *testing.T) {
	path := dbPath(t)
	clk := newFakeClock()

	db := openTestDB(t, path, clk)
	mustSet(t, db, "k", "v", 0)
	_ = db.Close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil { // simulated torn write
		t.Fatal(err)
	}
	_ = f.Close()

	db2 := openTestDB(t, path, clk)
	mustHave(t, db2, "k", "v")
	if got := db2.Recovery().TruncatedBytes; got != 5 {
		t.Fatalf("TruncatedBytes = %d, want 5", got)
	}
}

func TestDBRejectsUndecodableRecord(t *testing.T) {
	path := dbPath(t)

	// a record with a valid checksum but a payload that is not a Command
	w, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte{9}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	if _, err := OpenDB(path); !errors.Is(err, ErrBadCommand) {
		t.Fatalf("OpenDB err = %v, want ErrBadCommand", err)
	}
}

func TestDBEmptyKey(t *testing.T) {
	db := openTestDB(t, dbPath(t), newFakeClock())

	if err := db.Set("", []byte("v"), 0); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Set err = %v, want ErrEmptyKey", err)
	}
	if _, err := db.Delete(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete err = %v, want ErrEmptyKey", err)
	}
}

func TestDBClosed(t *testing.T) {
	db := openTestDB(t, dbPath(t), newFakeClock())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if err := db.Set("k", []byte("v"), 0); !errors.Is(err, wal.ErrClosed) {
		t.Fatalf("Set after Close = %v, want wal.ErrClosed", err)
	}
}

func TestDBConcurrentWritesReplayIdentically(t *testing.T) {
	path := dbPath(t)
	clk := newFakeClock()
	db := openTestDB(t, path, clk)

	const (
		goroutines = 8
		writes     = 20
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < writes; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i%5)
				if err := db.Set(key, []byte(fmt.Sprint(i)), 0); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	_ = db.Close()

	// Each goroutine owns its keys, so the last write per key is known.
	db2 := openTestDB(t, path, clk)
	for g := 0; g < goroutines; g++ {
		for j := 0; j < 5; j++ {
			mustHave(t, db2, fmt.Sprintf("g%d-k%d", g, j), fmt.Sprint(writes-5+j))
		}
	}
}
