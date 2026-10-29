package store

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sqmi511/raftkv/internal/wal"
)

// ErrEmptyKey is returned for operations with an empty key.
var ErrEmptyKey = errors.New("store: empty key")

// DB is a durable key-value store: every write is appended (and fsynced) to
// a write-ahead log before it is applied to the in-memory Store. Opening a DB
// replays the log to rebuild the state.
type DB struct {
	mu    sync.Mutex // serialises writes so log order == apply order
	store *Store
	log   *wal.WAL
}

// OpenDB opens (or creates) the WAL at walPath and replays it. Store options
// (clock, sweep interval) are passed through to the in-memory store.
func OpenDB(walPath string, opts ...Option) (*DB, error) {
	log, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}
	db := &DB{store: New(opts...), log: log}

	err = log.Replay(func(payload []byte) error {
		cmd, err := DecodeCommand(payload)
		if err != nil {
			return err
		}
		db.apply(cmd)
		return nil
	})
	if err != nil {
		db.store.Close()
		_ = log.Close()
		return nil, fmt.Errorf("store: replay: %w", err)
	}
	return db, nil
}

// Recovery reports what the WAL found when it was opened.
func (db *DB) Recovery() wal.Recovery { return db.log.Recovery() }

// Get returns the value for key. The boolean is false if it is missing or expired.
func (db *DB) Get(key string) ([]byte, bool) { return db.store.Get(key) }

// Set durably stores value under key. ttl <= 0 means no expiry.
func (db *DB) Set(key string, value []byte, ttl time.Duration) error {
	if key == "" {
		return ErrEmptyKey
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	cmd := Command{Op: OpSet, Key: key, Value: value}
	if ttl > 0 {
		cmd.ExpiresAt = db.store.now().Add(ttl).UnixNano()
	}
	if err := db.log.Append(cmd.Encode()); err != nil {
		return err
	}
	db.apply(cmd)
	return nil
}

// Delete durably removes key and reports whether a live key was removed.
// Deleting a missing key changes nothing and writes nothing to the log.
func (db *DB) Delete(key string) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.store.Get(key); !ok {
		return false, nil
	}
	if err := db.log.Append(Command{Op: OpDelete, Key: key}.Encode()); err != nil {
		return false, err
	}
	db.apply(Command{Op: OpDelete, Key: key})
	return true, nil
}

// Close stops the background sweeper and closes the log. Safe to call twice.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.store.Close()
	return db.log.Close()
}

// apply mutates the in-memory state. It is used both for live writes and for
// WAL replay, so both paths behave identically.
func (db *DB) apply(c Command) {
	switch c.Op {
	case OpSet:
		if c.ExpiresAt == 0 {
			db.store.Set(c.Key, c.Value, 0)
			return
		}
		remaining := time.Unix(0, c.ExpiresAt).Sub(db.store.now())
		if remaining <= 0 {
			// Already expired (e.g. during downtime). Delete rather than skip
			// so an older value for the same key is not resurrected.
			db.store.Delete(c.Key)
			return
		}
		db.store.Set(c.Key, c.Value, remaining)
	case OpDelete:
		db.store.Delete(c.Key)
	}
}
