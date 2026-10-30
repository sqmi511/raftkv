package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/sqmi511/raftkv/internal/store"
)

// ErrBadSnapshot is returned when a snapshot cannot be decoded.
var ErrBadSnapshot = errors.New("kv: malformed snapshot")

const snapshotVersion = 1

// Snapshot serializes the whole state machine: every live key (with its
// absolute expiry) AND the session table, so a node restored from a snapshot
// still recognises retried requests. The output is deterministic.
func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return encodeSnapshot(m.st.Dump(), m.sessions), nil
}

// Restore replaces the entire state with a snapshot produced by Snapshot. If
// the data is malformed it returns an error and leaves the state untouched.
func (m *Machine) Restore(data []byte) error {
	items, sessions, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st.Replace(items)
	m.sessions = sessions
	return nil
}

// Wire format:
//
//	version (1B)
//	uvarint(item count), then per item: uvarint(len key) key uvarint(len value) value expiresAt (8B, big endian)
//	uvarint(session count), then per session: uvarint(len id) id uvarint(lastRequest) uvarint(lastIndex) deleted (1B)
func encodeSnapshot(items []store.Item, sessions map[string]*session) []byte {
	buf := []byte{snapshotVersion}
	buf = binary.AppendUvarint(buf, uint64(len(items)))
	for _, it := range items {
		buf = binary.AppendUvarint(buf, uint64(len(it.Key)))
		buf = append(buf, it.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(it.Value)))
		buf = append(buf, it.Value...)
		buf = binary.BigEndian.AppendUint64(buf, uint64(it.ExpiresAt))
	}

	ids := make([]string, 0, len(sessions))
	for id := range sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	buf = binary.AppendUvarint(buf, uint64(len(ids)))
	for _, id := range ids {
		s := sessions[id]
		buf = binary.AppendUvarint(buf, uint64(len(id)))
		buf = append(buf, id...)
		buf = binary.AppendUvarint(buf, s.lastRequest)
		buf = binary.AppendUvarint(buf, s.lastIndex)
		if s.deleted {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	return buf
}

func decodeSnapshot(data []byte) ([]store.Item, map[string]*session, error) {
	if len(data) == 0 || data[0] != snapshotVersion {
		return nil, nil, fmt.Errorf("%w: unsupported or missing version", ErrBadSnapshot)
	}
	r := &snapReader{b: data[1:]}

	n := r.uvarint("item count")
	if r.err == nil && n > uint64(len(r.b)) {
		r.fail("item count exceeds the data")
	}
	var items []store.Item
	if r.err == nil {
		items = make([]store.Item, 0, n)
	}
	for i := uint64(0); i < n && r.err == nil; i++ {
		key := r.take(r.uvarint("key length"), "key")
		val := r.take(r.uvarint("value length"), "value")
		exp := r.u64("expiry")
		if r.err != nil {
			break
		}
		items = append(items, store.Item{
			Key:       string(key),
			Value:     append([]byte(nil), val...),
			ExpiresAt: int64(exp),
		})
	}

	sessions := make(map[string]*session)
	m := r.uvarint("session count")
	if r.err == nil && m > uint64(len(r.b)) {
		r.fail("session count exceeds the data")
	}
	for i := uint64(0); i < m && r.err == nil; i++ {
		id := r.take(r.uvarint("client id length"), "client id")
		last := r.uvarint("last request")
		idx := r.uvarint("last index")
		del := r.flag("deleted flag")
		if r.err != nil {
			break
		}
		sessions[string(id)] = &session{lastRequest: last, lastIndex: idx, deleted: del}
	}

	if r.err == nil && len(r.b) != 0 {
		r.fail("trailing bytes")
	}
	if r.err != nil {
		return nil, nil, r.err
	}
	return items, sessions, nil
}

// snapReader reads fields in order and remembers the first error, so the
// decoder can check once instead of after every field.
type snapReader struct {
	b   []byte
	err error
}

func (r *snapReader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ErrBadSnapshot, what)
	}
}

func (r *snapReader) uvarint(what string) uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("bad " + what)
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *snapReader) take(n uint64, what string) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.fail(what + " exceeds the data")
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *snapReader) u64(what string) uint64 {
	b := r.take(8, what)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (r *snapReader) flag(what string) bool {
	b := r.take(1, what)
	if r.err != nil {
		return false
	}
	if b[0] > 1 {
		r.fail("bad " + what)
		return false
	}
	return b[0] == 1
}
