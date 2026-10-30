// Package transport implements the gRPC transport: the client-facing KV API
// server and graceful-shutdown handling.
package transport

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiproto "github.com/sqmi511/raftkv/api/proto"
	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/store"
	"github.com/sqmi511/raftkv/internal/wal"
)

const maxTTLMillis = math.MaxInt64 / int64(time.Millisecond)

// Backend is what the gRPC layer needs from the storage side. *kv.Service
// (a Raft cluster node) implements it, and DBBackend adapts the standalone
// single-node database.
type Backend interface {
	Get(ctx context.Context, key string) (value []byte, found bool, err error)
	Set(ctx context.Context, clientID string, requestID uint64, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, clientID string, requestID uint64, key string) (deleted bool, err error)
}

// DBBackend serves a standalone store.DB. It has no leader and no request
// deduplication: a single node has nothing to redirect to, and the request
// identity is ignored.
type DBBackend struct{ db *store.DB }

// NewDBBackend wraps db.
func NewDBBackend(db *store.DB) *DBBackend { return &DBBackend{db: db} }

// Get implements Backend.
func (b *DBBackend) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := b.db.Get(key)
	return v, ok, nil
}

// Set implements Backend.
func (b *DBBackend) Set(_ context.Context, _ string, _ uint64, key string, value []byte, ttl time.Duration) error {
	return b.db.Set(key, value, ttl)
}

// Delete implements Backend.
func (b *DBBackend) Delete(_ context.Context, _ string, _ uint64, key string) (bool, error) {
	return b.db.Delete(key)
}

// ServerOption configures a KVServer.
type ServerOption func(*KVServer)

// WithLeaderResolver tells the server how to turn a Raft node ID into the
// client-facing address sent to clients as a leader hint. Without it, no
// hint is sent.
func WithLeaderResolver(resolve func(nodeID string) string) ServerOption {
	return func(s *KVServer) { s.resolve = resolve }
}

// KVServer implements the kvpb.KV gRPC service on top of a Backend.
type KVServer struct {
	kvpb.UnimplementedKVServer
	b       Backend
	resolve func(nodeID string) string
}

// NewKVServer returns a KV service backed by b.
func NewKVServer(b Backend, opts ...ServerOption) *KVServer {
	s := &KVServer{b: b}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Get implements kvpb.KVServer.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	v, ok, err := s.b.Get(ctx, req.GetKey())
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &kvpb.GetResponse{Found: ok, Value: v}, nil
}

// Set implements kvpb.KVServer.
func (s *KVServer) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	ttl := req.GetTtlMillis()
	if ttl < 0 || ttl > maxTTLMillis {
		return nil, status.Error(codes.InvalidArgument, "ttl_millis out of range")
	}
	err := s.b.Set(ctx, req.GetClientId(), req.GetRequestId(), req.GetKey(), req.GetValue(),
		time.Duration(ttl)*time.Millisecond)
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &kvpb.SetResponse{}, nil
}

// Delete implements kvpb.KVServer.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	deleted, err := s.b.Delete(ctx, req.GetClientId(), req.GetRequestId(), req.GetKey())
	if err != nil {
		return nil, s.fail(ctx, err)
	}
	return &kvpb.DeleteResponse{Deleted: deleted}, nil
}

// fail converts err to a gRPC status and, for "not leader" errors, attaches
// the leader hint as a trailer.
func (s *KVServer) fail(ctx context.Context, err error) error {
	var nl *kv.NotLeaderError
	if errors.As(err, &nl) && s.resolve != nil && nl.LeaderID != "" {
		if addr := s.resolve(nl.LeaderID); addr != "" {
			_ = grpc.SetTrailer(ctx, metadata.Pairs(apiproto.LeaderTrailer, addr))
		}
	}
	return toStatus(err)
}

// toStatus maps backend errors to gRPC status codes. Internal details are
// logged, not sent to clients.
//
// Everything a client should simply retry (not leader, leadership changed,
// shutting down) is UNAVAILABLE.
func toStatus(err error) error {
	var nl *kv.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return status.Error(codes.Unavailable, "not the leader")
	case errors.Is(err, kv.ErrLeadershipLost), errors.Is(err, kv.ErrOverwritten):
		return status.Error(codes.Unavailable, "leadership changed; retry")
	case errors.Is(err, kv.ErrStopped), errors.Is(err, wal.ErrClosed):
		return status.Error(codes.Unavailable, "server is shutting down")
	case errors.Is(err, store.ErrEmptyKey), errors.Is(err, wal.ErrRecordTooLarge), errors.Is(err, kv.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, kv.ErrStaleRequest):
		return status.Error(codes.FailedPrecondition, "request id is older than the client's latest request")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request cancelled")
	default:
		slog.Error("request failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// Serve runs the KV gRPC server for a standalone database on lis. See ServeBackend.
func Serve(ctx context.Context, lis net.Listener, db *store.DB, grace time.Duration) error {
	return ServeBackend(ctx, lis, NewDBBackend(db), grace)
}

// ServeBackend runs the KV gRPC server on lis until ctx is cancelled or the
// listener fails. On cancellation it stops accepting connections and waits up
// to grace for in-flight RPCs to finish before forcing them closed. It
// returns nil on a clean shutdown.
func ServeBackend(ctx context.Context, lis net.Listener, b Backend, grace time.Duration, opts ...ServerOption) error {
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, NewKVServer(b, opts...))

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case err := <-errCh:
		return err // listener failed before shutdown was requested
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(grace):
		srv.Stop() // grace period over: cut remaining connections
		<-stopped
	}
	return <-errCh
}
