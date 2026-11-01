package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sqmi511/raftkv/client"
)

// Client routes every operation to the group that owns the key, using one
// ordinary raftkv client per group (so each group gets leader discovery,
// retries and exactly-once writes). It is safe for concurrent use.
//
// The set of groups is fixed at creation. Changing it later moves ownership
// of some keys but does not move their data.
type Client struct {
	ring    *Ring
	clients map[string]*client.Client
}

// New creates a sharded client. base supplies the per-group client settings;
// its Addrs are ignored, and a non-empty ClientID becomes "<ClientID>/<group>".
func New(groups []Group, base client.Options) (*Client, error) {
	if len(groups) == 0 {
		return nil, ErrEmptyRing
	}
	c := &Client{ring: NewRing(0), clients: make(map[string]*client.Client, len(groups))}
	for _, g := range groups {
		if err := c.ring.Add(g.Name); err != nil {
			_ = c.Close()
			return nil, err
		}
		opts := base
		opts.Addrs = g.Addrs
		if base.ClientID != "" {
			opts.ClientID = base.ClientID + "/" + g.Name
		}
		cl, err := client.New(opts)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("shard: group %q: %w", g.Name, err)
		}
		c.clients[g.Name] = cl
	}
	return c, nil
}

// Locate returns the name of the group that owns key.
func (c *Client) Locate(key string) string {
	g, _ := c.ring.Locate(key) // never empty: New requires at least one group
	return g
}

// Get returns the value of key (a linearizable read within its group).
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.clients[c.Locate(key)].Get(ctx, key)
}

// Set stores value under key. ttl 0 means no expiry.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.clients[c.Locate(key)].Set(ctx, key, value, ttl)
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	return c.clients[c.Locate(key)].Delete(ctx, key)
}

// Close closes every group's client. It is idempotent.
func (c *Client) Close() error {
	var errs []error
	for _, cl := range c.clients {
		errs = append(errs, cl.Close())
	}
	return errors.Join(errs...)
}
