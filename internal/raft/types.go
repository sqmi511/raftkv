package raft

import "context"

// Entry is one record of the replicated log. Indexes start at 1.
//
// An entry with empty Data is a no-op: a new leader appends one to commit
// entries from earlier terms, and nodes never pass it to the state machine.
// Entries are immutable: never modify the Data of an entry you received.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

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

// AppendEntriesArgs is the AppendEntries RPC request: a heartbeat when
// Entries is empty, log replication otherwise. Receivers must treat it as
// read-only.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64 // index of the entry immediately before Entries
	PrevLogTerm  uint64 // term of that entry
	Entries      []Entry
	LeaderCommit uint64 // leader's commit index
}

// AppendEntriesReply is the AppendEntries RPC response.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// ConflictIndex is set when the log-consistency check fails. It is the
	// index the leader should retry from: either just past the end of the
	// follower's log, or the first index of the follower's conflicting term.
	// It is 0 when the rejection was not caused by a log mismatch.
	ConflictIndex uint64
}

// Transport sends RPCs to other nodes. Implementations must honour ctx
// cancellation and return an error if the peer cannot be reached.
type Transport interface {
	RequestVote(ctx context.Context, to string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
