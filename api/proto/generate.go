// Package proto holds the protobuf definitions. The generated Go code lives
// in the kvpb (client API) and raftpb (node-to-node) subpackages and is
// committed to the repository.
//
// Regenerate with: go generate ./api/proto
package proto

//go:generate protoc --proto_path=. --go_out=../.. --go_opt=module=github.com/sqmi511/raftkv --go-grpc_out=../.. --go-grpc_opt=module=github.com/sqmi511/raftkv kv.proto
//go:generate protoc --proto_path=. --go_out=../.. --go_opt=module=github.com/sqmi511/raftkv --go-grpc_out=../.. --go-grpc_opt=module=github.com/sqmi511/raftkv raft.proto
