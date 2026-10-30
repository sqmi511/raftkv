package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	apiproto "github.com/sqmi511/raftkv/api/proto"
	"github.com/sqmi511/raftkv/api/proto/kvpb"
	"github.com/sqmi511/raftkv/internal/kv"
	"github.com/sqmi511/raftkv/internal/store"
)

// fakeBackend returns a configured error and records what it was asked.
type fakeBackend struct {
	mu       sync.Mutex
	err      error
	clientID string
	reqID    uint64
	ttl      time.Duration
}

func (f *fakeBackend) Get(context.Context, string) ([]byte, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	return []byte("v"), true, nil
}

func (f *fakeBackend) Set(_ context.Context, clientID string, requestID uint64, _ string, _ []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clientID, f.reqID, f.ttl = clientID, requestID, ttl
	return f.err
}

func (f *fakeBackend) Delete(_ context.Context, clientID string, requestID uint64, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clientID, f.reqID = clientID, requestID
	return f.err == nil, f.err
}

func (f *fakeBackend) recorded() (string, uint64, time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clientID, f.reqID, f.ttl
}

func startBackend(t *testing.T, b Backend, opts ...ServerOption) kvpb.KVClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeBackend(ctx, lis, b, time.Second, opts...) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return dial(t, lis.Addr().String())
}

func TestBackendErrorsMapToStatusCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not leader", &kv.NotLeaderError{}, codes.Unavailable},
		{"leadership lost", kv.ErrLeadershipLost, codes.Unavailable},
		{"overwritten", kv.ErrOverwritten, codes.Unavailable},
		{"stopped", kv.ErrStopped, codes.Unavailable},
		{"invalid request", kv.ErrInvalidRequest, codes.InvalidArgument},
		{"empty key", store.ErrEmptyKey, codes.InvalidArgument},
		{"stale request", kv.ErrStaleRequest, codes.FailedPrecondition},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"canceled", context.Canceled, codes.Canceled},
		{"unexpected", errors.New("boom"), codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Wrapped on purpose: the mapping must use errors.Is/As.
			c := startBackend(t, &fakeBackend{err: fmt.Errorf("wrapped: %w", tc.err)})
			_, err := c.Set(testCtx(t), &kvpb.SetRequest{Key: "k", ClientId: "c", RequestId: 1})
			wantCode(t, err, tc.want)
		})
	}
}

func TestLeaderHintTrailer(t *testing.T) {
	notLeader := &fakeBackend{err: &kv.NotLeaderError{LeaderID: "node-2"}}

	t.Run("resolver provides an address", func(t *testing.T) {
		c := startBackend(t, notLeader, WithLeaderResolver(func(id string) string {
			return map[string]string{"node-2": "10.0.0.2:7000"}[id]
		}))
		var trailer metadata.MD
		_, err := c.Get(testCtx(t), &kvpb.GetRequest{Key: "k"}, grpc.Trailer(&trailer))
		wantCode(t, err, codes.Unavailable)
		if got := trailer.Get(apiproto.LeaderTrailer); len(got) != 1 || got[0] != "10.0.0.2:7000" {
			t.Fatalf("leader trailer = %v, want [10.0.0.2:7000]", got)
		}
	})

	t.Run("no resolver means no hint", func(t *testing.T) {
		c := startBackend(t, notLeader)
		var trailer metadata.MD
		_, err := c.Get(testCtx(t), &kvpb.GetRequest{Key: "k"}, grpc.Trailer(&trailer))
		wantCode(t, err, codes.Unavailable)
		if got := trailer.Get(apiproto.LeaderTrailer); len(got) != 0 {
			t.Fatalf("unexpected leader trailer %v", got)
		}
	})

	t.Run("unknown leader means no hint", func(t *testing.T) {
		c := startBackend(t, &fakeBackend{err: &kv.NotLeaderError{}},
			WithLeaderResolver(func(string) string { return "should-not-be-used" }))
		var trailer metadata.MD
		_, err := c.Set(testCtx(t), &kvpb.SetRequest{Key: "k", ClientId: "c", RequestId: 1}, grpc.Trailer(&trailer))
		wantCode(t, err, codes.Unavailable)
		if got := trailer.Get(apiproto.LeaderTrailer); len(got) != 0 {
			t.Fatalf("unexpected leader trailer %v", got)
		}
	})
}

func TestRequestIdentityReachesBackend(t *testing.T) {
	fb := &fakeBackend{}
	c := startBackend(t, fb)

	_, err := c.Set(testCtx(t), &kvpb.SetRequest{Key: "k", Value: []byte("v"), TtlMillis: 1500, ClientId: "client-a", RequestId: 7})
	if err != nil {
		t.Fatal(err)
	}
	if id, req, ttl := fb.recorded(); id != "client-a" || req != 7 || ttl != 1500*time.Millisecond {
		t.Fatalf("backend saw (%q, %d, %v), want (client-a, 7, 1.5s)", id, req, ttl)
	}

	if _, err := c.Delete(testCtx(t), &kvpb.DeleteRequest{Key: "k", ClientId: "client-b", RequestId: 9}); err != nil {
		t.Fatal(err)
	}
	if id, req, _ := fb.recorded(); id != "client-b" || req != 9 {
		t.Fatalf("backend saw (%q, %d), want (client-b, 9)", id, req)
	}
}
