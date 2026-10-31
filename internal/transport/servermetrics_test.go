package transport

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sqmi511/raftkv/internal/metrics"
)

func TestServerMetricsInterceptor(t *testing.T) {
	reg := metrics.NewRegistry()
	sm := NewServerMetrics(reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	intercept := sm.UnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/raftkv.v1.KV/Get"}

	okHandler := func(context.Context, any) (any, error) { return "resp", nil }
	failHandler := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Unavailable, "not the leader")
	}

	resp, err := intercept(context.Background(), nil, info, okHandler)
	if err != nil || resp != "resp" {
		t.Fatalf("interceptor changed the result: %v, %v", resp, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := intercept(context.Background(), nil, info, failHandler); status.Code(err) != codes.Unavailable {
			t.Fatalf("interceptor changed the error: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`raftkv_grpc_requests_total{method="/raftkv.v1.KV/Get",code="OK"} 1`,
		`raftkv_grpc_requests_total{method="/raftkv.v1.KV/Get",code="Unavailable"} 2`,
		`raftkv_grpc_request_duration_seconds_bucket{method="/raftkv.v1.KV/Get",le="+Inf"} 3`,
		`raftkv_grpc_request_duration_seconds_count{method="/raftkv.v1.KV/Get"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q\n%s", want, body)
		}
	}
}
