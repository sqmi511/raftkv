package transport

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/internal/metrics"
)

// ServerMetrics records a request counter and a latency histogram for every
// unary gRPC call, client API and Raft RPCs alike, and logs each at debug level.
type ServerMetrics struct {
	requests *metrics.CounterVec
	duration *metrics.HistogramVec
	logger   *slog.Logger
}

// NewServerMetrics registers the gRPC metrics on reg. Call it once per
// registry. A nil logger uses slog.Default().
func NewServerMetrics(reg *metrics.Registry, logger *slog.Logger) *ServerMetrics {
	if logger == nil {
		logger = slog.Default()
	}
	return &ServerMetrics{
		requests: reg.NewCounterVec("raftkv_grpc_requests_total",
			"gRPC requests handled, by method and result code.", "method", "code"),
		duration: reg.NewHistogramVec("raftkv_grpc_request_duration_seconds",
			"gRPC request latency in seconds, by method.", metrics.DefaultBuckets, "method"),
		logger: logger,
	}
}

// UnaryInterceptor returns the interceptor to install on the gRPC server.
func (m *ServerMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)

		code := status.Code(err).String()
		m.requests.With(info.FullMethod, code).Inc()
		m.duration.With(info.FullMethod).Observe(elapsed.Seconds())
		m.logger.Debug("rpc", "method", info.FullMethod, "code", code, "duration", elapsed)
		return resp, err
	}
}
