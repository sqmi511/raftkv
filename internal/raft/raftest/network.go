// Package raftest provides an in-memory network for testing Raft nodes in a
// single process, with the ability to cut nodes off from the cluster.
package raftest

import (
	"context"
	"errors"
	"sync"

	"github.com/sqmi511/raftkv/internal/raft"
)

// ErrUnreachable is returned when an RPC cannot be delivered.
var ErrUnreachable = errors.New("raftest: destination unreachable")

// Network connects registered nodes. The zero value is not usable; use
// NewNetwork.
type Network struct {
	mu       sync.RWMutex
	nodes    map[string]*raft.Node
	isolated map[string]bool
}

// NewNetwork returns an empty, fully connected network.
func NewNetwork() *Network {
	return &Network{
		nodes:    make(map[string]*raft.Node),
		isolated: make(map[string]bool),
	}
}

// Register makes node reachable under id.
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

// Transport returns the raft.Transport that node `from` should use.
func (nw *Network) Transport(from string) raft.Transport {
	return endpoint{nw: nw, from: from}
}

func (nw *Network) route(from, to string) (*raft.Node, error) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	node, ok := nw.nodes[to]
	if !ok || nw.isolated[from] || nw.isolated[to] {
		return nil, ErrUnreachable
	}
	return node, nil
}

type endpoint struct {
	nw   *Network
	from string
}

func (e endpoint) RequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	node, err := e.nw.route(e.from, to)
	if err != nil {
		return nil, err
	}
	reply := node.HandleRequestVote(args)
	// The reply travels back over the same network: drop it if the link
	// was cut while the request was being handled.
	if _, err := e.nw.route(to, e.from); err != nil {
		return nil, err
	}
	return reply, nil
}

func (e endpoint) AppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	node, err := e.nw.route(e.from, to)
	if err != nil {
		return nil, err
	}
	reply := node.HandleAppendEntries(args)
	if _, err := e.nw.route(to, e.from); err != nil {
		return nil, err
	}
	return reply, nil
}
