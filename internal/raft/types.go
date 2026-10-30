package raft

import "context"

// RequestVoteArgs is the RequestVote RPC request (Raft paper, figure 2).
// Receivers must treat it as read-only.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request. In this task it is only
// used as a heartbeat; log fields are added in the next task.
// Receivers must treat it as read-only.
type AppendEntriesArgs struct {
	Term     uint64
	LeaderID string
}

// AppendEntriesReply is the AppendEntries RPC response.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// Transport sends RPCs to other nodes. Implementations must honour ctx
// cancellation and return an error if the peer cannot be reached.
type Transport interface {
	RequestVote(ctx context.Context, to string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
