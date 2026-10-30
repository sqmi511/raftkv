package proto

// LeaderTrailer is the gRPC trailer key a node uses to tell a client where
// the leader's client-facing address is, when it rejects a request because it
// is not the leader.
const LeaderTrailer = "raftkv-leader"
