package transport

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/api/proto/raftpb"
	"github.com/sqmi511/raftkv/internal/kv"
)

// ClusterOptions tunes ServeCluster. The zero value is valid.
type ClusterOptions struct {
	// Metrics, if set, records metrics for every RPC.
	Metrics *ServerMetrics
	// ResolveLeader turns a Raft node ID into the address sent to clients as
	// a leader hint. Default: the ID itself, which is correct when nodes use
	// their advertised address as ID.
	ResolveLeader func(nodeID string) string
}

// ServeCluster runs one cluster node's gRPC server on lis: the client KV API
// and the node-to-node Raft service share the listener. It blocks until ctx
// is cancelled or the listener fails. On cancellation it stops accepting
// connections and waits up to grace for in-flight RPCs before forcing them
// closed. It returns nil on a clean shutdown.
func ServeCluster(ctx context.Context, lis net.Listener, svc *kv.Service, grace time.Duration, opts ClusterOptions) error {
	resolve := opts.ResolveLeader
	if resolve == nil {
		resolve = func(id string) string { return id }
	}

	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageSize),
		grpc.MaxSendMsgSize(MaxMessageSize),
	}
	if opts.Metrics != nil {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(opts.Metrics.UnaryInterceptor()))
	}
	srv := grpc.NewServer(serverOpts...)
	kvpb.RegisterKVServer(srv, NewKVServer(svc, WithLeaderResolver(resolve)))
	raftpb.RegisterRaftServer(srv, NewRaftServer(svc.Node()))

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
