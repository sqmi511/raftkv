// Package kvtest starts whole raftkv clusters inside one test process: real
// gRPC servers on loopback for clients, and an in-memory network between the
// Raft nodes so tests can partition them. A node's ID is its client-facing
// gRPC address, which makes leader hints directly usable by clients.
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
}

type node struct {
	svc    *kv.Service
	cancel context.CancelFunc
	served chan struct{} // closed when the gRPC server has returned
	once   sync.Once
}

// Cluster is a running cluster. All nodes are stopped when the test ends.
type Cluster struct {
	t     *testing.T
	net   *raftest.Network
	addrs []string
	nodes map[string]*node

	mu   sync.Mutex
	down map[string]bool // isolated or stopped
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
	c := &Cluster{
		t:     t,
		net:   raftest.NewNetwork(),
		nodes: make(map[string]*node),
		down:  make(map[string]bool),
	}

	listeners := make([]net.Listener, size)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		c.addrs = append(c.addrs, lis.Addr().String())
	}

	for i, addr := range c.addrs {
		var peers []string
		for _, p := range c.addrs {
			if p != addr {
				peers = append(peers, p)
			}
		}
		svc, err := kv.New(kv.Config{
			SnapshotThreshold: opts.SnapshotThreshold,
			Raft: raft.Config{
				ID:                 addr,
				Peers:              peers,
				Transport:          c.net.Transport(addr),
				Storage:            raft.NewMemStorage(),
				ElectionTimeoutMin: electionMin,
				ElectionTimeoutMax: electionMax,
				HeartbeatInterval:  heartbeat,
				Seed:               int64(i + 1),
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		n := &node{svc: svc, cancel: cancel, served: make(chan struct{})}
		c.nodes[addr] = n
		c.net.Register(addr, svc.Node())

		lis := listeners[i]
		go func() {
			defer close(n.served)
			_ = transport.ServeBackend(ctx, lis, svc, shutdownGrace,
				transport.WithLeaderResolver(func(id string) string { return id }))
		}()
	}

	for _, n := range c.nodes {
		n.svc.Start()
	}
	t.Cleanup(func() {
		for _, a := range c.addrs {
			c.Stop(a)
		}
	})
	return c
}

// Addrs returns every node's client-facing address (which is also its Raft ID).
func (c *Cluster) Addrs() []string { return append([]string(nil), c.addrs...) }

// Service returns the service running on addr.
func (c *Cluster) Service(addr string) *kv.Service { return c.nodes[addr].svc }

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

// Stop kills a node: its gRPC server and Raft node stop, so clients get
// "connection refused". It is idempotent.
func (c *Cluster) Stop(addr string) {
	n := c.nodes[addr]
	n.once.Do(func() {
		n.cancel()
		<-n.served
		n.svc.Stop()
		c.net.Isolate(addr)
		c.setDown(addr, true)
	})
}

// WaitLeader waits until the reachable nodes agree on a term, exactly one of
// them is its leader, and that leader has committed its whole log (so it can
// serve reads). It returns the leader's address.
func (c *Cluster) WaitLeader() string {
	c.t.Helper()
	deadline := time.Now().Add(leaderWait)
	for {
		if leader, ok := c.settledLeader(); ok {
			return leader
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("no stable leader after %v\n%s", leaderWait, c.dump())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *Cluster) settledLeader() (string, bool) {
	live := c.liveAddrs()
	if len(live) == 0 {
		return "", false
	}
	leader := ""
	var term uint64
	for _, a := range live {
		st := c.nodes[a].svc.Node().Status()
		if st.State == raft.Leader {
			if leader != "" {
				return "", false
			}
			leader, term = a, st.Term
		}
	}
	if leader == "" {
		return "", false
	}
	for _, a := range live {
		if c.nodes[a].svc.Node().Status().Term != term {
			return "", false
		}
	}
	st := c.nodes[leader].svc.Node().Status()
	if st.LastIndex < 1 || st.CommitIndex < st.LastIndex {
		return "", false
	}
	return leader, true
}

func (c *Cluster) liveAddrs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, a := range c.addrs {
		if !c.down[a] {
			out = append(out, a)
		}
	}
	return out
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

func (c *Cluster) dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, a := range c.addrs {
		fmt.Fprintf(&b, "%s down=%v %+v\n", a, c.down[a], c.nodes[a].svc.Node().Status())
	}
	return b.String()
}
