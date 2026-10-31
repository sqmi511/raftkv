package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sqmi511/raftkv/api/proto/raftpb"
	"github.com/sqmi511/raftkv/internal/raft"
)

// MaxMessageSize is the largest gRPC message accepted or sent between nodes.
// An InstallSnapshot carries a whole snapshot in one message, so this bounds
// the state size a lagging follower can be brought up to date with.
const MaxMessageSize = 64 << 20

var errRaftClientClosed = errors.New("transport: raft client closed")

// RaftServer exposes a Raft node's RPC handlers over gRPC.
type RaftServer struct {
	raftpb.UnimplementedRaftServer
	node *raft.Node
}

// NewRaftServer returns the gRPC service for node.
func NewRaftServer(node *raft.Node) *RaftServer { return &RaftServer{node: node} }

// RequestVote implements raftpb.RaftServer.
func (s *RaftServer) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	r := s.node.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})
	return &raftpb.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

// AppendEntries implements raftpb.RaftServer.
func (s *RaftServer) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	var entries []raft.Entry
	if n := len(req.GetEntries()); n > 0 {
		entries = make([]raft.Entry, n)
		for i, e := range req.GetEntries() {
			entries[i] = raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Data: e.GetData()}
		}
	}
	r := s.node.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.GetTerm(),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: req.GetLeaderCommit(),
	})
	return &raftpb.AppendEntriesResponse{Term: r.Term, Success: r.Success, ConflictIndex: r.ConflictIndex}, nil
}

// InstallSnapshot implements raftpb.RaftServer.
func (s *RaftServer) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	r := s.node.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              req.GetTerm(),
		LeaderID:          req.GetLeaderId(),
		LastIncludedIndex: req.GetLastIncludedIndex(),
		LastIncludedTerm:  req.GetLastIncludedTerm(),
		Data:              req.GetData(),
	})
	return &raftpb.InstallSnapshotResponse{Term: r.Term, Success: r.Success}, nil
}

// RaftClient is the real raft.Transport: it sends RPCs to peers over gRPC.
// Connections are created lazily, one per peer, and reused.
//
// Targets use the "passthrough" resolver so the host name is resolved by the
// dialer on every connection attempt. That keeps working when a peer's DNS
// name appears late (containers starting in any order) or its IP changes.
// The reconnect backoff is short so a restarted peer is reached again quickly.
//
// NOTE: plaintext transport; TLS/mTLS is a planned hardening step.
type RaftClient struct {
	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

var _ raft.Transport = (*RaftClient)(nil)

// NewRaftClient returns a client with no open connections.
func NewRaftClient() *RaftClient {
	return &RaftClient{conns: make(map[string]*grpc.ClientConn)}
}

func (c *RaftClient) peer(to string) (raftpb.RaftClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errRaftClientClosed
	}
	cc, ok := c.conns[to]
	if !ok {
		var err error
		cc, err = grpc.NewClient("passthrough:///"+to,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageSize),
				grpc.MaxCallSendMsgSize(MaxMessageSize),
			),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   2 * time.Second,
				},
				MinConnectTimeout: time.Second,
			}),
		)
		if err != nil {
			return nil, err
		}
		c.conns[to] = cc
	}
	return raftpb.NewRaftClient(cc), nil
}

// RequestVote implements raft.Transport.
func (c *RaftClient) RequestVote(ctx context.Context, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	cl, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	resp, err := cl.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.GetTerm(), VoteGranted: resp.GetVoteGranted()}, nil
}

// AppendEntries implements raft.Transport.
func (c *RaftClient) AppendEntries(ctx context.Context, to string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	cl, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	req := &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		LeaderCommit: args.LeaderCommit,
	}
	if len(args.Entries) > 0 {
		req.Entries = make([]*raftpb.LogEntry, len(args.Entries))
		for i, e := range args.Entries {
			req.Entries[i] = &raftpb.LogEntry{Index: e.Index, Term: e.Term, Data: e.Data}
		}
	}
	resp, err := cl.AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.GetTerm(),
		Success:       resp.GetSuccess(),
		ConflictIndex: resp.GetConflictIndex(),
	}, nil
}

// InstallSnapshot implements raft.Transport.
func (c *RaftClient) InstallSnapshot(ctx context.Context, to string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	cl, err := c.peer(to)
	if err != nil {
		return nil, err
	}
	resp, err := cl.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: resp.GetTerm(), Success: resp.GetSuccess()}, nil
}

// Close closes every connection. It is idempotent; later RPCs fail.
func (c *RaftClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var errs []error
	for _, cc := range c.conns {
		errs = append(errs, cc.Close())
	}
	c.conns = nil
	return errors.Join(errs...)
}
