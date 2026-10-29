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
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/internal/store"
	"github.com/sqmi511/raftkv/internal/wal"
)

const maxTTLMillis = math.MaxInt64 / int64(time.Millisecond)

// KVServer implements the kvpb.KV gRPC service on top of a durable store.
type KVServer struct {
	kvpb.UnimplementedKVServer
	db *store.DB
}

// NewKVServer returns a KV service backed by db.
func NewKVServer(db *store.DB) *KVServer { return &KVServer{db: db} }

// Get implements kvpb.KVServer.
func (s *KVServer) Get(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	v, ok := s.db.Get(req.GetKey())
	return &kvpb.GetResponse{Found: ok, Value: v}, nil
}

// Set implements kvpb.KVServer.
func (s *KVServer) Set(_ context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	ttl := req.GetTtlMillis()
	if ttl < 0 || ttl > maxTTLMillis {
		return nil, status.Error(codes.InvalidArgument, "ttl_millis out of range")
	}
	if err := s.db.Set(req.GetKey(), req.GetValue(), time.Duration(ttl)*time.Millisecond); err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.SetResponse{}, nil
}

// Delete implements kvpb.KVServer.
func (s *KVServer) Delete(_ context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	deleted, err := s.db.Delete(req.GetKey())
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.DeleteResponse{Deleted: deleted}, nil
}

// toStatus maps storage errors to gRPC status codes. Internal details are
// logged, not sent to clients.
func toStatus(err error) error {
	switch {
	case errors.Is(err, store.ErrEmptyKey), errors.Is(err, wal.ErrRecordTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, wal.ErrClosed):
		return status.Error(codes.Unavailable, "server is shutting down")
	default:
		slog.Error("request failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

// Serve runs the KV gRPC server on lis until ctx is cancelled or the listener
// fails. On cancellation it stops accepting connections and waits up to grace
// for in-flight RPCs to finish before forcing them closed. It returns nil on a
// clean shutdown.
func Serve(ctx context.Context, lis net.Listener, db *store.DB, grace time.Duration) error {
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, NewKVServer(db))

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
