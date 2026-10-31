// Package kvtest starts whole raftkv clusters inside one test process: real
// gRPC servers on loopback for clients, and an in-memory network between the
// Raft nodes so tests can partition them, drop and delay messages, and kill
// and restart nodes. A node's ID is its client-facing gRPC address, which
// makes leader hints directly usable by clients.
//
// Each node keeps its Raft storage across Stop and Restart, like a machine
// whose disk survives a crash.
package kvtest

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/raft/raftest"
	"github.com/sqmi511/raftkv/internal/transport"
)

const (
	electionMin   = 200 * time.Millisecond
	electionMax   = 400 * time.Millisecond
	heartbeat     = 40 * time.Millisecond
	shutdownGrace = 200 * time.Millisecond
	leaderWait    = 20 * time.Second
)

// Options tunes a test cluster. The zero value gives the defaults.
type Options struct {
	// SnapshotThreshold is how many applied entries trigger a snapshot.
	// 0 selects kv.DefaultSnapshotThreshold.
	SnapshotThreshold uint64
	// Seed seeds the network's drop and delay decisions. 0 means time-based.
	Seed int64
}

type node struct {
	svc    *kv.Service
	cancel context.CancelFunc
	served chan struct{} // closed when the gRPC server has returned
	dead   bool          // guarded by Cluster.mu
}

// Cluster is a running cluster. All nodes are stopped when the test ends.
type Cluster struct {
	t        *testing.T
	net      *raftest.Network
	opts     Options
	addrs    []string
	storages map[string]*raft.MemStorage // fixed after construction

	mu     sync.Mutex
	nodes  map[string]*node
	down   map[string]bool // isolated or stopped
	starts int64
}

// NewCluster starts size nodes with default options and returns immediately;
// use WaitLeader to wait for an election.
func NewCluster(t *testing.T, size int) *Cluster {
	t.Helper()
	return NewClusterWith(t, size, Options{})
}

// NewClusterWith is NewCluster with explicit options.
func NewClusterWith(t *testing.T, size int, opts Options) *Cluster {
	t.Helper()
	nw := raftest.NewNetwork()
	if opts.Seed != 0 {
		nw = raftest.NewNetworkSeeded(opts.Seed)
	}
	c := &Cluster{
		t:        t,
		net:      nw,
		opts:     opts,
		storages: make(map[string]*raft.MemStorage),
		nodes:    make(map[string]*node),
		down:     make(map[string]bool),
	}

	listeners := make([]net.Listener, size)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		addr := lis.Addr().String()
		c.addrs = append(c.addrs, addr)
		c.storages[addr] = raft.NewMemStorage()
	}

	t.Cleanup(func() {
		for _, a := range c.addrs {
			c.Stop(a)
		}
	})

	for i, addr := range c.addrs {
		n, err := c.launch(addr, listeners[i])
		if err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.nodes[addr] = n
		c.mu.Unlock()
	}
	return c
}

// launch builds a service for addr on top of its (possibly pre-existing)
// storage, registers it on the network, and starts it.
func (c *Cluster) launch(addr string, lis net.Listener) (*node, error) {
	var peers []string
	for _, p := range c.addrs {
		if p != addr {
			peers = append(peers, p)
		}
	}
	c.mu.Lock()
	c.starts++
	seed := c.starts
	c.mu.Unlock()

	svc, err := kv.New(kv.Config{
		SnapshotThreshold: c.opts.SnapshotThreshold,
		Raft: raft.Config{
			ID:                 addr,
			Peers:              peers,
			Transport:          c.net.Transport(addr),
			Storage:            c.storages[addr],
			ElectionTimeoutMin: electionMin,
			ElectionTimeoutMax: electionMax,
			HeartbeatInterval:  heartbeat,
			Seed:               seed,
		},
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &node{svc: svc, cancel: cancel, served: make(chan struct{})}
	c.net.Register(addr, svc.Node())
	go func() {
		defer close(n.served)
		_ = transport.ServeBackend(ctx, lis, svc, shutdownGrace,
			transport.WithLeaderResolver(func(id string) string { return id }))
	}()
	svc.Start()
	return n, nil
}

// Addrs returns every node's client-facing address (which is also its Raft ID).
func (c *Cluster) Addrs() []string { return append([]string(nil), c.addrs...) }

// Service returns the service currently running on addr (after a Restart, the
// new one). A stopped node's last service is returned until it is restarted.
func (c *Cluster) Service(addr string) *kv.Service {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[addr].svc
}

// Followers returns every address except leader.
func (c *Cluster) Followers(leader string) []string {
	var out []string
	for _, a := range c.addrs {
		if a != leader {
			out = append(out, a)
		}
	}
	return out
}

// IsDown reports whether addr is stopped or isolated.
func (c *Cluster) IsDown(addr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.down[addr]
}

// Isolate cuts addr off from the other Raft nodes. Clients can still reach
// it, and it keeps running, like a machine behind a network partition.
func (c *Cluster) Isolate(addr string) {
	c.net.Isolate(addr)
	c.setDown(addr, true)
}

// Heal reconnects a node cut off by Isolate. Do not use it on a stopped node.
func (c *Cluster) Heal(addr string) {
	c.net.Heal(addr)
	c.setDown(addr, false)
}

// Partition splits the Raft network into the given groups; nodes not listed
// stay together. Clients are not affected.
func (c *Cluster) Partition(groups ...[]string) { c.net.Partition(groups...) }

// HealPartition removes a partition created by Partition.
func (c *Cluster) HealPartition() { c.net.HealPartition() }

// SetDropRate makes each Raft message get lost with probability p.
func (c *Cluster) SetDropRate(p float64) { c.net.SetDropRate(p) }

// SetDelay delays each Raft message by a random time in [min, max].
func (c *Cluster) SetDelay(min, max time.Duration) { c.net.SetDelay(min, max) }

// ResetFaults removes partitions, message loss and delay.
func (c *Cluster) ResetFaults() { c.net.ResetFaults() }

// Stop kills a node: its gRPC server and Raft node stop, so clients get
// "connection refused". Its storage is kept for Restart. It is idempotent.
func (c *Cluster) Stop(addr string) {
	c.mu.Lock()
	n := c.nodes[addr]
	if n == nil || n.dead {
		c.mu.Unlock()
		return
	}
	n.dead = true
	c.down[addr] = true
	c.mu.Unlock()

	c.net.Isolate(addr)
	n.cancel()
	<-n.served
	n.svc.Stop()
}

// Restart starts a stopped node again from its own storage, with an empty
// state machine that is rebuilt from the snapshot and log, on the same address.
func (c *Cluster) Restart(addr string) error {
	c.mu.Lock()
	old := c.nodes[addr]
	if old == nil || !old.dead {
		c.mu.Unlock()
		return fmt.Errorf("kvtest: node %s is not stopped", addr)
	}
	c.mu.Unlock()

	lis, err := listenRetry(addr)
	if err != nil {
		return err
	}
	n, err := c.launch(addr, lis)
	if err != nil {
		_ = lis.Close()
		return err
	}
	c.mu.Lock()
	c.nodes[addr] = n
	delete(c.down, addr)
	c.mu.Unlock()
	c.net.Heal(addr)
	return nil
}

func listenRetry(addr string) (net.Listener, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("kvtest: cannot listen on %s again: %w", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// WaitLeader waits until the reachable nodes agree on a term, exactly one of
// them is its leader, and that leader has committed its whole log (so it can
// serve reads). It returns the leader's address. Heal partitions first.
func (c *Cluster) WaitLeader() string {
	c.t.Helper()
	deadline := time.Now().Add(leaderWait)
	for {
		if leader, ok := c.settledLeader(); ok {
			return leader
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no stable leader after %v\n%s", leaderWait, c.Dump())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type liveNode struct {
	addr string
	svc  *kv.Service
}

func (c *Cluster) live() []liveNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []liveNode
	for _, a := range c.addrs {
		if n := c.nodes[a]; n != nil && !c.down[a] {
			out = append(out, liveNode{a, n.svc})
		}
	}
	return out
}

func (c *Cluster) settledLeader() (string, bool) {
	live := c.live()
	if len(live) == 0 {
		return "", false
	}
	var leader *liveNode
	var term uint64
	for i, ln := range live {
		st := ln.svc.Node().Status()
		if st.State == raft.Leader {
			if leader != nil {
				return "", false
			}
			leader, term = &live[i], st.Term
		}
	}
	if leader == nil {
		return "", false
	}
	for _, ln := range live {
		if ln.svc.Node().Status().Term != term {
			return "", false
		}
	}
	st := leader.svc.Node().Status()
	if st.LastIndex < 1 || st.CommitIndex < st.LastIndex {
		return "", false
	}
	return leader.addr, true
}

func (c *Cluster) setDown(addr string, v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v {
		c.down[addr] = true
	} else {
		delete(c.down, addr)
	}
}

// Dump describes every node's state, for failure messages.
func (c *Cluster) Dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, a := range c.addrs {
		fmt.Fprintf(&b, "%s down=%v %+v\n", a, c.down[a], c.nodes[a].svc.Node().Status())
	}
	return b.String()
}
