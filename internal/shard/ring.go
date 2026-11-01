// Package shard spreads keys over several independent raftkv clusters
// ("groups") with a consistent-hash ring, and provides a client that routes
// each key to the group that owns it.
package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// DefaultVirtualNodes is how many points each group gets on the ring. More
// points give a more even spread.
const DefaultVirtualNodes = 256

var (
	// ErrEmptyRing is returned when a key is located on a ring without groups.
	ErrEmptyRing = errors.New("shard: ring has no groups")
	// ErrInvalidGroup is returned for a malformed or duplicate group.
	ErrInvalidGroup = errors.New("shard: invalid group")
)

type point struct {
	hash  uint64
	group string
}

// Ring is a consistent-hash ring. Adding a group only takes keys away from
// existing groups (never moves keys between them), and removing a group only
// gives that group's keys to the others. It is safe for concurrent use.
type Ring struct {
	vnodes int

	mu     sync.RWMutex
	groups map[string]struct{}
	points []point // sorted by hash, then group name, so the layout never depends on insertion order
}

// NewRing returns an empty ring. vnodes <= 0 selects DefaultVirtualNodes.
func NewRing(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVirtualNodes
	}
	return &Ring{vnodes: vnodes, groups: make(map[string]struct{})}
}

// hash64 maps a string to a point on the ring. SHA-256 spreads even
// near-identical inputs ("key-1", "key-2") evenly.
func hash64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// Add puts a group on the ring.
func (r *Ring) Add(group string) error {
	if group == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidGroup)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.groups[group]; dup {
		return fmt.Errorf("%w: %q is already on the ring", ErrInvalidGroup, group)
	}
	r.groups[group] = struct{}{}
	for i := 0; i < r.vnodes; i++ {
		r.points = append(r.points, point{hash: hash64(group + "#" + strconv.Itoa(i)), group: group})
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].group < r.points[j].group
	})
	return nil
}

// Remove takes a group off the ring and reports whether it was there.
func (r *Ring) Remove(group string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.groups[group]; !ok {
		return false
	}
	delete(r.groups, group)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.group != group {
			kept = append(kept, p)
		}
	}
	r.points = kept
	return true
}

// Groups returns the group names in sorted order.
func (r *Ring) Groups() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.groups))
	for g := range r.groups {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// Locate returns the group that owns key.
func (r *Ring) Locate(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", ErrEmptyRing
	}
	h := hash64(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrap around the ring
	}
	return r.points[i].group, nil
}
