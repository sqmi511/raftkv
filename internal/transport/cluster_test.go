package transport_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/sqmi511/raftkv/api/proto/raftpb"
	"github.com/sqmi511/raftkv/client"
	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/raft"
	"github.com/sqmi511/raftkv/internal/transport"
)

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// deadTransport is for nodes that must never contact peers.
type deadTransport struct{}

func (deadTransport) RequestVote(context.Context, string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, errors.New("dead")
}

func (deadTransport) AppendEntries(context.Context, string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, errors.New("dead")
}

func (deadTransport) InstallSnapshot(context.Context, string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nil, errors.New("dead")
}

func TestRaftRPCsRoundTripOverGRPC(t *testing.T) {
	node, err := raft.NewNode(raft.Config{ID: "n1", Peers: []string{"n2"}, Transport: deadTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	raftpb.RegisterRaftServer(srv, transport.NewRaftServer(node))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	rc := transport.NewRaftClient()
	t.Cleanup(func() { _ = rc.Close() })
	addr := lis.Addr().String()
	ctx := ctxTimeout(t, 20*time.Second)

	// RequestVote: first candidate wins the vote, a second one in the same term does not.
	vr, err := rc.RequestVote(ctx, addr, &raft.RequestVoteArgs{Term: 3, CandidateID: "n2"})
	if err != nil || !vr.VoteGranted || vr.Term != 3 {
		t.Fatalf("RequestVote = %+v, %v; want a vote at term 3", vr, err)
	}
	vr, err = rc.RequestVote(ctx, addr, &raft.RequestVoteArgs{Term: 3, CandidateID: "n9"})
	if err != nil || vr.VoteGranted {
		t.Fatalf("second RequestVote = %+v, %v; want it refused", vr, err)
	}

	// AppendEntries: entries (including an empty no-op) arrive intact.
	ar, err := rc.AppendEntries(ctx, addr, &raft.AppendEntriesArgs{
		Term: 3, LeaderID: "n2", LeaderCommit: 2,
		Entries: []raft.Entry{{Index: 1, Term: 3, Data: []byte("a")}, {Index: 2, Term: 3}},
	})
	if err != nil || !ar.Success {
		t.Fatalf("AppendEntries = %+v, %v; want success", ar, err)
	}
	if st := node.Status(); st.LastIndex != 2 || st.CommitIndex != 2 || st.LeaderID != "n2" {
		t.Fatalf("node status after AppendEntries = %+v", st)
	}

	// A log mismatch carries its ConflictIndex back.
	ar, err = rc.AppendEntries(ctx, addr, &raft.AppendEntriesArgs{Term: 3, LeaderID: "n2", PrevLogIndex: 9, PrevLogTerm: 3})
	if err != nil || ar.Success || ar.ConflictIndex != 3 {
		t.Fatalf("mismatching AppendEntries = %+v, %v; want ConflictIndex 3", ar, err)
	}

	// InstallSnapshot: a large payload goes through in one message.
	sr, err := rc.InstallSnapshot(ctx, addr, &raft.InstallSnapshotArgs{
		Term: 3, LeaderID: "n2", LastIncludedIndex: 5, LastIncludedTerm: 3,
		Data: bytes.Repeat([]byte("x"), 1<<20),
	})
	if err != nil || !sr.Success {
		t.Fatalf("InstallSnapshot = %+v, %v; want success", sr, err)
	}
	if got := node.Status().SnapshotIndex; got != 5 {
		t.Fatalf("SnapshotIndex = %d, want 5", got)
	}
}

func TestRaftClientErrors(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := lis.Addr().String()
	_ = lis.Close() // nothing listens here any more

	rc := transport.NewRaftClient()
	if _, err := rc.RequestVote(ctxTimeout(t, 3*time.Second), dead, &raft.RequestVoteArgs{Term: 1, CandidateID: "x"}); err == nil {
		t.Fatal("an RPC to a dead address succeeded")
	}

	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if _, err := rc.RequestVote(ctxTimeout(t, time.Second), dead, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("an RPC on a closed client succeeded")
	}
}

// ---- a real three-node cluster over real gRPC ---------------------------------

type gnode struct {
	svc    *kv.Service
	rc     *transport.RaftClient
	st     *raft.WALStorage
	cancel context.CancelFunc
	served chan struct{}
	once   sync.Once
}

func startGNode(t *testing.T, addr string, lis net.Listener, peers []string, dir string) *gnode {
	t.Helper()
	st, err := raft.OpenWALStorage(filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	rc := transport.NewRaftClient()
	svc, err := kv.New(kv.Config{
		SnapshotThreshold: 10, // small, so the test exercises snapshots over gRPC
		Raft: raft.Config{
			ID:                 addr,
			Peers:              peers,
			Transport:          rc,
			Storage:            st,
			ElectionTimeoutMin: 200 * time.Millisecond,
			ElectionTimeoutMax: 400 * time.Millisecond,
			HeartbeatInterval:  40 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &gnode{svc: svc, rc: rc, st: st, cancel: cancel, served: make(chan struct{})}
	go func() {
		defer close(n.served)
		_ = transport.ServeCluster(ctx, lis, svc, time.Second, transport.ClusterOptions{})
	}()
	svc.Start()
	return n
}

// stop kills the node; its data directory stays, like a crashed machine's disk.
func (n *gnode) stop() {
	n.once.Do(func() {
		n.cancel()
		<-n.served
		n.svc.Stop()
		_ = n.rc.Close()
		_ = n.st.Close()
	})
}

func peersOf(addrs []string, self int) []string {
	var out []string
	for i, a := range addrs {
		if i != self {
			out = append(out, a)
		}
	}
	return out
}

func waitLeader(t *testing.T, live []*gnode) *gnode {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range live {
			if n.svc.Node().Status().State == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader was elected")
	return nil
}

func listenRetry(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis
		}
		if time.Now().After(deadline) {
			t.Fatalf("cannot re-listen on %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestThreeNodeClusterOverGRPC(t *testing.T) {
	const size = 3
	lis := make([]net.Listener, size)
	addrs := make([]string, size)
	for i := range lis {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[i], addrs[i] = l, l.Addr().String()
	}
	dirs := make([]string, size)
	nodes := make([]*gnode, size)
	for i := range nodes {
		dirs[i] = t.TempDir()
		nodes[i] = startGNode(t, addrs[i], lis[i], peersOf(addrs, i), dirs[i])
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})

	cl, err := client.New(client.Options{Addrs: addrs, MaxAttempts: 40})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	ctx := ctxTimeout(t, 3*time.Minute)

	leader := waitLeader(t, nodes)

	// More writes than the snapshot threshold, so logs get compacted.
	const n = 25
	for i := 0; i < n; i++ {
		if err := cl.Set(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Set k%d: %v", i, err)
		}
	}
	checkAll := func() {
		t.Helper()
		for i := 0; i < n; i++ {
			v, ok, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
			if err != nil || !ok || string(v) != fmt.Sprintf("v%d", i) {
				t.Fatalf("Get(k%d) = %q, %v, %v", i, v, ok, err)
			}
		}
	}
	checkAll()

	// Kill the leader. A new one takes over and no write is lost.
	li := -1
	for i, nd := range nodes {
		if nd == leader {
			li = i
		}
	}
	leader.stop()
	if err := cl.Set(ctx, "after", []byte("x"), 0); err != nil {
		t.Fatalf("Set after the leader died: %v", err)
	}
	checkAll()

	// Restart the dead node from its own disk. It rejoins and catches up.
	nodes[li] = startGNode(t, addrs[li], listenRetry(t, addrs[li]), peersOf(addrs, li), dirs[li])
	restarted := nodes[li]

	deadline := time.Now().Add(30 * time.Second)
	for {
		a, okA := restarted.svc.StaleGet("after")
		b, okB := restarted.svc.StaleGet(fmt.Sprintf("k%d", n-1))
		if okA && okB && string(a) == "x" && string(b) == fmt.Sprintf("v%d", n-1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the restarted node never caught up; status %+v", restarted.svc.Node().Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := restarted.svc.Node().Status().Err; err != nil {
		t.Fatalf("the restarted node reports a failure: %v", err)
	}
}
