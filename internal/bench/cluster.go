package bench

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/transport"
)

const shutdownGrace = 200 * time.Millisecond

// ClusterOptions configures an in-process cluster.
type ClusterOptions struct {
	Size int    // number of nodes; default 3
	Dir  string // base directory for node data (required); each node gets a subdirectory

	SnapshotThreshold uint64 // 0 selects the built-in default

	// Defaults match the server's defaults: 300ms, 600ms and 100ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

func (o *ClusterOptions) fill() error {
	if o.Size == 0 {
		o.Size = 3
	}
	if o.Size < 1 {
		return fmt.Errorf("bench: cluster size must be at least 1")
	}
	if o.Dir == "" {
		return fmt.Errorf("bench: cluster Dir is required")
	}
	if o.ElectionTimeoutMin == 0 {
		o.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if o.ElectionTimeoutMax == 0 {
		o.ElectionTimeoutMax = 600 * time.Millisecond
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 100 * time.Millisecond
	}
	return nil
}

type node struct {
	svc     *kv.Service
	rc      *transport.RaftClient
	st      *raft.WALStorage
	cancel  context.CancelFunc
	served  chan struct{} // closed when the gRPC server has returned
	crashed bool          // guarded by Cluster.mu
}

// Cluster is a set of real raftkv nodes in one process: real gRPC on
// loopback between nodes and to clients, and a WAL on disk per node.
type Cluster struct {
	opts  ClusterOptions
	addrs []string
	dirs  map[string]string

	mu    sync.Mutex
	nodes map[string]*node
}

// StartCluster starts a cluster. Call WaitHealthy before using it, and Close
// when finished.
func StartCluster(opts ClusterOptions) (*Cluster, error) {
	if err := opts.fill(); err != nil {
		return nil, err
	}
	c := &Cluster{opts: opts, dirs: make(map[string]string), nodes: make(map[string]*node)}

	listeners := make([]net.Listener, opts.Size)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, l := range listeners[:i] {
				_ = l.Close()
			}
			return nil, err
		}
		listeners[i] = lis
		addr := lis.Addr().String()
		c.addrs = append(c.addrs, addr)
		dir := filepath.Join(opts.Dir, fmt.Sprintf("node%d", i+1))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			for _, l := range listeners[:i+1] {
				_ = l.Close()
			}
			return nil, err
		}
		c.dirs[addr] = dir
	}

	for i, addr := range c.addrs {
		n, err := c.launch(addr, listeners[i])
		if err != nil {
			for _, l := range listeners[i:] {
				_ = l.Close()
			}
			c.Close()
			return nil, err
		}
		c.mu.Lock()
		c.nodes[addr] = n
		c.mu.Unlock()
	}
	return c, nil
}

// launch builds and starts the node for addr on top of whatever its data
// directory holds (an empty directory is a fresh node).
func (c *Cluster) launch(addr string, lis net.Listener) (*node, error) {
	var peers []string
	for _, p := range c.addrs {
		if p != addr {
			peers = append(peers, p)
		}
	}
	st, err := raft.OpenWALStorage(filepath.Join(c.dirs[addr], "raft.wal"))
	if err != nil {
		return nil, err
	}
	rc := transport.NewRaftClient()
	svc, err := kv.New(kv.Config{
		SnapshotThreshold: c.opts.SnapshotThreshold,
		Raft: raft.Config{
			ID:                 addr,
			Peers:              peers,
			Transport:          rc,
			Storage:            st,
			ElectionTimeoutMin: c.opts.ElectionTimeoutMin,
			ElectionTimeoutMax: c.opts.ElectionTimeoutMax,
			HeartbeatInterval:  c.opts.HeartbeatInterval,
		},
	})
	if err != nil {
		_ = rc.Close()
		_ = st.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &node{svc: svc, rc: rc, st: st, cancel: cancel, served: make(chan struct{})}
	go func() {
		defer close(n.served)
		_ = transport.ServeCluster(ctx, lis, svc, shutdownGrace, transport.ClusterOptions{})
	}()
	svc.Start()
	return n, nil
}

// Addrs returns every node's address (also its Raft ID and client address).
func (c *Cluster) Addrs() []string { return append([]string(nil), c.addrs...) }

// Status returns the Raft status of the node at addr.
func (c *Cluster) Status(addr string) raft.Status {
	c.mu.Lock()
	n := c.nodes[addr]
	c.mu.Unlock()
	if n == nil {
		return raft.Status{}
	}
	return n.svc.Node().Status()
}

// Crash kills a node the way a power cut would: its Raft node stops at once
// (no more heartbeats, no replies), then its server closes. Its data
// directory stays, so Restart brings it back.
func (c *Cluster) Crash(addr string) error {
	c.mu.Lock()
	n := c.nodes[addr]
	switch {
	case n == nil:
		c.mu.Unlock()
		return fmt.Errorf("bench: unknown node %s", addr)
	case n.crashed:
		c.mu.Unlock()
		return fmt.Errorf("bench: node %s is already crashed", addr)
	}
	n.crashed = true
	c.mu.Unlock()

	n.svc.Stop()
	n.cancel()
	_ = n.rc.Close()
	_ = n.st.Close()
	return nil
}

// Restart starts a crashed node again from its own data directory.
func (c *Cluster) Restart(addr string) error {
	c.mu.Lock()
	n := c.nodes[addr]
	c.mu.Unlock()
	if n == nil || !n.crashed {
		return fmt.Errorf("bench: node %s is not crashed", addr)
	}
	<-n.served // the old server has released the port

	lis, err := listenRetry(addr)
	if err != nil {
		return err
	}
	fresh, err := c.launch(addr, lis)
	if err != nil {
		_ = lis.Close()
		return err
	}
	c.mu.Lock()
	c.nodes[addr] = fresh
	c.mu.Unlock()
	return nil
}

// Close stops every node. It is safe to call more than once.
func (c *Cluster) Close() {
	for _, addr := range c.addrs {
		c.mu.Lock()
		n := c.nodes[addr]
		c.mu.Unlock()
		if n == nil {
			continue
		}
		_ = c.Crash(addr) // an error just means it was already crashed
		<-n.served
	}
}

func listenRetry(addr string) (net.Listener, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("bench: cannot listen on %s again: %w", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
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
		if n := c.nodes[a]; n != nil && !n.crashed {
			out = append(out, liveNode{a, n.svc})
		}
	}
	return out
}

// Leader returns the running node that believes it leads in the highest term.
func (c *Cluster) Leader() (addr string, st raft.Status, ok bool) {
	for _, ln := range c.live() {
		s := ln.svc.Node().Status()
		if s.State == raft.Leader && (!ok || s.Term > st.Term) {
			addr, st, ok = ln.addr, s, true
		}
	}
	return addr, st, ok
}

// WaitHealthy blocks until every node is running, they agree on the term, one
// is leader, and the leader's log is fully committed and known to all nodes.
// It returns the leader's address.
func (c *Cluster) WaitHealthy(ctx context.Context) (string, error) {
	for {
		if leader, ok := c.healthy(); ok {
			return leader, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("bench: cluster did not become healthy: %w\n%s", ctx.Err(), c.dump())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *Cluster) healthy() (string, bool) {
	live := c.live()
	if len(live) != len(c.addrs) {
		return "", false
	}
	statuses := make([]raft.Status, len(live))
	leaders := 0
	var leader string
	var lst raft.Status
	for i, ln := range live {
		st := ln.svc.Node().Status()
		statuses[i] = st
		if st.Err != nil {
			return "", false
		}
		if st.State == raft.Leader {
			leaders++
			leader, lst = ln.addr, st
		}
	}
	if leaders != 1 || lst.LastIndex < 1 || lst.CommitIndex < lst.LastIndex {
		return "", false
	}
	for _, st := range statuses {
		if st.Term != lst.Term || st.CommitIndex < lst.CommitIndex {
			return "", false
		}
	}
	return leader, true
}

func (c *Cluster) dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, a := range c.addrs {
		n := c.nodes[a]
		if n == nil {
			fmt.Fprintf(&b, "%s (not started)\n", a)
			continue
		}
		fmt.Fprintf(&b, "%s crashed=%v %+v\n", a, n.crashed, n.svc.Node().Status())
	}
	return b.String()
}
