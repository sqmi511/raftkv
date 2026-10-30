// Package client is the Go client for a raftkv cluster. It finds the leader,
// follows redirects, retries transient failures, and makes retries safe by
// tagging every write with a client ID and a request ID, which the cluster
// uses to apply each write exactly once.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apiproto "github.com/sqmi511/raftkv/api/proto"
	"github.com/sqmi511/raftkv/api/proto/kvpb"
)

var (
	// ErrNoAddrs is returned by New when no server address is given.
	ErrNoAddrs = errors.New("client: at least one server address is required")
	// ErrInvalidTTL is returned for a TTL that is negative or below 1ms.
	ErrInvalidTTL = errors.New("client: ttl must be 0 (no expiry) or at least 1ms")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("client: closed")
)

// Options configures a Client.
type Options struct {
	// Addrs lists cluster members (host:port). One is enough to bootstrap:
	// a follower tells the client where the leader is.
	Addrs []string
	// ClientID identifies this client for deduplication. Default: a random
	// ID. It must be unique per client instance; never share one between
	// two running clients.
	ClientID string
	// AttemptTimeout bounds one RPC attempt, so a partitioned or hung node
	// cannot stall the client. Default 2s.
	AttemptTimeout time.Duration
	// MaxAttempts is the number of tries per operation. Default 10.
	MaxAttempts int
	// BackoffMin/BackoffMax bound the pause between failed attempts, which
	// doubles each time. Defaults 25ms and 500ms.
	BackoffMin time.Duration
	BackoffMax time.Duration
	// DialOptions are added after the defaults (plaintext transport), so
	// they can override them, e.g. with TLS credentials.
	DialOptions []grpc.DialOption
}

// Client talks to a raftkv cluster. It is safe for concurrent use; writes
// from one Client are issued one at a time (see Set).
type Client struct {
	id   string
	opts Options

	wmu     sync.Mutex // serialises writes: one outstanding request ID per client
	nextReq uint64     // guarded by wmu

	mu     sync.Mutex // guards the fields below
	closed bool
	conns  map[string]*grpc.ClientConn
	last   string // the address that last served a request successfully
}

// New validates opts and creates a client. Connections are made lazily.
func New(opts Options) (*Client, error) {
	if len(opts.Addrs) == 0 {
		return nil, ErrNoAddrs
	}
	for _, a := range opts.Addrs {
		if a == "" {
			return nil, errors.New("client: empty server address")
		}
	}
	opts.Addrs = append([]string(nil), opts.Addrs...)
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 2 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 10
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = 25 * time.Millisecond
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 500 * time.Millisecond
	}
	if opts.BackoffMax < opts.BackoffMin {
		return nil, errors.New("client: BackoffMax must not be below BackoffMin")
	}

	id := opts.ClientID
	if id == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("client: generate id: %w", err)
		}
		id = hex.EncodeToString(b[:])
	}
	return &Client{id: id, opts: opts, conns: make(map[string]*grpc.ClientConn)}, nil
}

// ID returns this client's deduplication ID.
func (c *Client) ID() string { return c.id }

// Close closes all connections. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var errs []error
	for _, cc := range c.conns {
		errs = append(errs, cc.Close())
	}
	c.conns = nil
	return errors.Join(errs...)
}

// Get returns the value for key. The read is linearizable.
func (c *Client) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	err = c.do(ctx, func(actx context.Context, kvc kvpb.KVClient, tr *metadata.MD) error {
		resp, err := kvc.Get(actx, &kvpb.GetRequest{Key: key}, grpc.Trailer(tr))
		if err != nil {
			return err
		}
		value, found = resp.GetValue(), resp.GetFound()
		return nil
	})
	return value, found, err
}

// Set stores value under key. ttl 0 means no expiry.
//
// Retries inside one call reuse the same request ID, so the write is applied
// at most once even if responses are lost or the leader changes. If Set
// returns an error, the write's outcome is unknown: it may or may not have
// been applied.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl < 0 || (ttl > 0 && ttl < time.Millisecond) {
		return ErrInvalidTTL
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.nextReq++
	reqID := c.nextReq

	return c.do(ctx, func(actx context.Context, kvc kvpb.KVClient, tr *metadata.MD) error {
		_, err := kvc.Set(actx, &kvpb.SetRequest{
			Key: key, Value: value, TtlMillis: ttl.Milliseconds(),
			ClientId: c.id, RequestId: reqID,
		}, grpc.Trailer(tr))
		return err
	})
}

// Delete removes key and reports whether it existed. Like Set, it is applied
// at most once, and a retried Delete reports the original answer.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.nextReq++
	reqID := c.nextReq

	var deleted bool
	err := c.do(ctx, func(actx context.Context, kvc kvpb.KVClient, tr *metadata.MD) error {
		resp, err := kvc.Delete(actx, &kvpb.DeleteRequest{Key: key, ClientId: c.id, RequestId: reqID}, grpc.Trailer(tr))
		if err != nil {
			return err
		}
		deleted = resp.GetDeleted()
		return nil
	})
	return deleted, err
}

type call func(ctx context.Context, kvc kvpb.KVClient, trailer *metadata.MD) error

// do runs fn against the cluster until it succeeds, fails permanently, or
// runs out of attempts or time.
//
// A "not leader" reply that names a new address is followed immediately; any
// other transient failure moves to the next address after a growing pause.
func (c *Client) do(ctx context.Context, fn call) error {
	target := c.start()
	tried := make(map[string]bool)
	backoff := c.opts.BackoffMin
	var lastErr error

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return ctxFailure(err, lastErr)
		}
		kvc, err := c.rpcClient(target)
		if err != nil {
			return err
		}
		tried[target] = true

		var trailer metadata.MD
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		err = fn(actx, kvc, &trailer)
		cancel()

		if err == nil {
			c.remember(target)
			return nil
		}
		lastErr = err
		if !retryable(ctx, err) || attempt >= c.opts.MaxAttempts {
			return err
		}

		if hint := leaderHint(trailer); hint != "" && !tried[hint] {
			target = hint // a new address to try: no need to wait
			continue
		}
		target = c.next(target)
		if err := sleep(ctx, backoff); err != nil {
			return ctxFailure(err, lastErr)
		}
		if backoff *= 2; backoff > c.opts.BackoffMax {
			backoff = c.opts.BackoffMax
		}
	}
}

// retryable reports whether err is worth another attempt. UNAVAILABLE covers
// "not leader", leadership changes and unreachable servers; DEADLINE_EXCEEDED
// is retried only when it was our per-attempt timeout, not the caller's deadline.
func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

func ctxFailure(ctxErr, lastErr error) error {
	if lastErr == nil {
		return ctxErr
	}
	return fmt.Errorf("%w (last error: %v)", ctxErr, lastErr)
}

func leaderHint(md metadata.MD) string {
	if vals := md.Get(apiproto.LeaderTrailer); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Client) start() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last != "" {
		return c.last
	}
	return c.opts.Addrs[0]
}

func (c *Client) remember(addr string) {
	c.mu.Lock()
	c.last = addr
	c.mu.Unlock()
}

// next returns the configured address after cur, wrapping around.
func (c *Client) next(cur string) string {
	for i, a := range c.opts.Addrs {
		if a == cur {
			return c.opts.Addrs[(i+1)%len(c.opts.Addrs)]
		}
	}
	return c.opts.Addrs[0]
}

func (c *Client) rpcClient(addr string) (kvpb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	cc, ok := c.conns[addr]
	if !ok {
		// NOTE: plaintext by default; pass DialOptions for TLS.
		dial := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.opts.DialOptions...)
		var err error
		if cc, err = grpc.NewClient(addr, dial...); err != nil {
			return nil, err
		}
		c.conns[addr] = cc
	}
	return kvpb.NewKVClient(cc), nil
}
