// Package raftest provides an in-memory network for testing Raft nodes in a
// single process. It can cut nodes off, split the cluster into partitions,
// drop messages and delay them.
package raftest

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/sqmi511/raftkv/internal/raft"
)

var (
	// ErrUnreachable is returned when an RPC cannot be delivered.
	ErrUnreachable = errors.New("raftest: destination unreachable")
	// ErrDropped is returned when the network loses a message. Real senders
	// would time out; reporting it immediately keeps tests fast, and Raft
	// treats both the same way.
	ErrDropped = errors.New("raftest: message dropped")
)

// Network connects registered nodes. The zero value is not usable; use
// NewNetwork or NewNetworkSeeded.
type Network struct {
	mu       sync.RWMutex
	nodes    map[string]*raft.Node
	isolated map[string]bool
	group    map[string]int // partition group; nodes not listed are in group 0
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration

	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewNetwork returns an empty, fully connected network.
func NewNetwork() *Network { return NewNetworkSeeded(time.Now().UnixNano()) }

// NewNetworkSeeded is NewNetwork with a fixed seed for drop and delay decisions.
func NewNetworkSeeded(seed int64) *Network {
	return &Network{
		nodes:    make(map[string]*raft.Node),
		isolated: make(map[string]bool),
		group:    make(map[string]int),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register makes node reachable under id. Registering an id again replaces
// the node (used to simulate a restart).
func (nw *Network) Register(id string, node *raft.Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = node
}

// Isolate cuts id off from every other node, in both directions. The node
// keeps running, like a machine behind a network partition.
func (nw *Network) Isolate(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.isolated[id] = true
}

// Heal reconnects an isolated node.
func (nw *Network) Heal(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.isolated, id)
}

// Partition splits the network: nodes can only talk to nodes in the same
// group. Nodes not listed in any group stay together in one extra group. A
// new call replaces the previous partition.
func (nw *Network) Partition(groups ...[]string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.group = make(map[string]int)
	for i, g := range groups {
		for _, id := range g {
			nw.group[id] = i + 1
		}
	}
}

// HealPartition removes the partition. Isolated nodes stay isolated.
func (nw *Network) HealPartition() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.group = make(map[string]int)
}

// SetDropRate makes every message (request or reply) get lost with
// probability p, clamped to [0, 1].
func (nw *Network) SetDropRate(p float64) {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dropRate = p
}

// SetDelay delays every message (request and reply separately) by a random
// duration in [min, max]. SetDelay(0, 0) turns delay off.
func (nw *Network) SetDelay(min, max time.Duration) {
	if min < 0 {
		min = 0
	}
	if max < min {
		max = min
	}
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.minDelay, nw.maxDelay = min, max
}

// ResetFaults removes the partition, message loss and delay. Isolation is
// left alone.
func (nw *Network) ResetFaults() {
	nw.mu.Lock()
	nw.group = make(map[string]int)
	nw.dropRate = 0
	nw.minDelay, nw.maxDelay = 0, 0
	nw.mu.Unlock()
}

// Transport returns the raft.Transport that node `from` should use.
func (nw *Network) Transport(from string) raft.Transport {
	return endpoint{nw: nw, from: from}
}

func (nw *Network) route(from, to string) (*raft.Node, error) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	node, ok := nw.nodes[to]
	if !ok || nw.isolated[from] || nw.isolated[to] || nw.group[from] != nw.group[to] {
		return nil, ErrUnreachable
	}
	return node, nil
}

// lose decides whether the next message is dropped.
func (nw *Network) lose() bool {
	nw.mu.RLock()
	p := nw.dropRate
	nw.mu.RUnlock()
	if p == 0 {
		return false
	}
	nw.rngMu.Lock()
	defer nw.rngMu.Unlock()
	return nw.rng.Float64() < p
}

// pause waits out the configured delay, or returns early if ctx ends.
func (nw *Network) pause(ctx context.Context) error {
	nw.mu.RLock()
	lo, hi := nw.minDelay, nw.maxDelay
	nw.mu.RUnlock()
	if hi == 0 {
		return nil
	}
	d := lo
	if span := hi - lo; span > 0 {
		nw.rngMu.Lock()
		d += time.Duration(nw.rng.Int63n(int64(span) + 1))
		nw.rngMu.Unlock()
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type endpoint struct {
	nw   *Network
	from string
}

// call simulates one RPC: the request may be lost or delayed, the handler
// runs on the destination, and then the reply may be lost or delayed too. A
// lost reply means the work was done but the caller never finds out.
func (e endpoint) call(ctx context.Context, to string, handle func(*raft.Node)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.nw.lose() {
		return ErrDropped
	}
	if err := e.nw.pause(ctx); err != nil {
		return err
	}
	node, err := e.nw.route(e.from, to)
	if err != nil {
		return err
	}
	handle(node)

	// The reply travels back over the same network: it is dropped if the
	// link was cut while the request was being handled.
	if e.nw.lose() {
		return ErrDropped
	}
	if err := e.nw.pause(ctx); err != nil {
		return err
	}
	_, err = e.nw.route(to, e.from)
	return err
}

func (e endpoint) RequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply *raft.RequestVoteReply
	if err := e.call(ctx, to, func(n *raft.Node) { reply = n.HandleRequestVote(args) }); err != nil {
		return nil, err
	}
	return reply, nil
}

func (e endpoint) AppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply *raft.AppendEntriesReply
	if err := e.call(ctx, to, func(n *raft.Node) { reply = n.HandleAppendEntries(args) }); err != nil {
		return nil, err
	}
	return reply, nil
}

func (e endpoint) InstallSnapshot(ctx context.Context, to string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var reply *raft.InstallSnapshotReply
	if err := e.call(ctx, to, func(n *raft.Node) { reply = n.HandleInstallSnapshot(args) }); err != nil {
		return nil, err
	}
	return reply, nil
}
