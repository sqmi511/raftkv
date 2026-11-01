// Package dashboard watches a raftkv cluster from the outside: it polls every
// node's /status endpoint, summarises the cluster, records notable changes as
// events, and serves a live web page.
package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	maxEvents      = 30
	maxStatusBytes = 1 << 20
)

// ErrInvalidTarget is wrapped by errors about malformed node addresses.
var ErrInvalidTarget = errors.New("dashboard: invalid target")

// NodeView is what the dashboard knows about one node.
type NodeView struct {
	Target    string  `json:"target"`
	Reachable bool    `json:"reachable"`
	Error     string  `json:"error,omitempty"` // why the node could not be polled
	LatencyMS float64 `json:"latency_ms"`

	ID            string `json:"id,omitempty"`
	State         string `json:"state,omitempty"`
	Term          uint64 `json:"term"`
	Leader        string `json:"leader,omitempty"` // who this node believes leads
	LastIndex     uint64 `json:"last_index"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Ready         bool   `json:"ready"`
	NodeError     string `json:"node_error,omitempty"` // a failure the node itself reports

	// Stale is set for a node that claims leadership in a term older than
	// the newest term seen: a deposed leader that has not noticed yet.
	Stale bool `json:"stale"`
}

func (v NodeView) label() string {
	if v.ID != "" {
		return v.ID
	}
	return v.Target
}

// Summary describes the cluster as a whole.
type Summary struct {
	Total     int    `json:"total"`
	Reachable int    `json:"reachable"`
	Quorum    bool   `json:"quorum"`  // a majority of nodes is reachable
	Leader    string `json:"leader"`  // the one current leader, "" if none or ambiguous
	Term      uint64 `json:"term"`    // highest term among reachable nodes
	Healthy   bool   `json:"healthy"` // quorum and a leader
}

// Event is one notable change.
type Event struct {
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"` // info, up, down, role, leader, warn
	Message string    `json:"message"`
}

// Snapshot is one complete picture of the cluster. Once published it is
// never modified.
type Snapshot struct {
	Time    time.Time  `json:"time"`
	Nodes   []NodeView `json:"nodes"`
	Summary Summary    `json:"summary"`
	Events  []Event    `json:"events"` // newest first
}

// Options configures a Poller.
type Options struct {
	// Targets are base URLs of the nodes' ops endpoints, e.g.
	// "http://127.0.0.1:8001" (see ParseTargets).
	Targets  []string
	Interval time.Duration // default 500ms
	Timeout  time.Duration // per request, default 1s
}

// Poller polls the nodes and publishes snapshots.
type Poller struct {
	targets  []string
	interval time.Duration
	timeout  time.Duration
	client   *http.Client

	mu          sync.Mutex
	snap        Snapshot
	has         bool
	prev        []NodeView
	prevSummary Summary
	events      []Event
	subs        map[chan Snapshot]struct{}
}

// NewPoller validates opts and returns a Poller. Call Run to start it.
func NewPoller(opts Options) (*Poller, error) {
	if len(opts.Targets) == 0 {
		return nil, fmt.Errorf("%w: no targets", ErrInvalidTarget)
	}
	if opts.Interval <= 0 {
		opts.Interval = 500 * time.Millisecond
	}
	if opts.Timeout <= 0 {
		opts.Timeout = time.Second
	}
	return &Poller{
		targets:  append([]string(nil), opts.Targets...),
		interval: opts.Interval,
		timeout:  opts.Timeout,
		client: &http.Client{
			// A status endpoint has no reason to redirect: don't follow.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		subs: make(map[chan Snapshot]struct{}),
	}, nil
}

// ParseTargets turns a comma-separated list such as
// "127.0.0.1:8001, http://node2:8000" into normalized base URLs.
func ParseTargets(list string) ([]string, error) {
	seen := make(map[string]bool)
	var out []string
	for _, raw := range strings.Split(list, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.Contains(raw, "://") {
			raw = "http://" + raw
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %v", ErrInvalidTarget, raw, err)
		}
		switch {
		case u.Scheme != "http" && u.Scheme != "https", u.Hostname() == "":
			return nil, fmt.Errorf("%w: %q: want http(s)://host:port", ErrInvalidTarget, raw)
		case (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil:
			return nil, fmt.Errorf("%w: %q: give only scheme://host:port", ErrInvalidTarget, raw)
		}
		t := u.Scheme + "://" + u.Host
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: the list is empty", ErrInvalidTarget)
	}
	return out, nil
}

// Run polls until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	p.PollOnce(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.PollOnce(ctx)
		}
	}
}

// PollOnce polls every node once (in parallel), publishes and returns the result.
func (p *Poller) PollOnce(ctx context.Context) Snapshot {
	views := make([]NodeView, len(p.targets))
	var wg sync.WaitGroup
	for i, target := range p.targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			views[i] = p.fetch(ctx, target)
		}()
	}
	wg.Wait()
	return p.update(time.Now(), views)
}

// Snapshot returns the latest snapshot; ok is false before the first poll.
func (p *Poller) Snapshot() (snap Snapshot, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snap, p.has
}

// Subscribe returns a channel of snapshots. It holds at most one: a slow
// reader skips to the newest. The latest snapshot (if any) is delivered
// immediately. Call cancel when done; the channel is never closed.
func (p *Poller) Subscribe() (snaps <-chan Snapshot, cancel func()) {
	ch := make(chan Snapshot, 1)
	p.mu.Lock()
	p.subs[ch] = struct{}{}
	if p.has {
		ch <- p.snap
	}
	p.mu.Unlock()
	return ch, func() {
		p.mu.Lock()
		delete(p.subs, ch)
		p.mu.Unlock()
	}
}

// statusBody mirrors the JSON served by a node's /status endpoint.
type statusBody struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	Term          uint64 `json:"term"`
	Leader        string `json:"leader"`
	LastIndex     uint64 `json:"last_index"`
	CommitIndex   uint64 `json:"commit_index"`
	AppliedIndex  uint64 `json:"applied_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	Ready         bool   `json:"ready"`
	Error         string `json:"error"`
}

func (p *Poller) fetch(ctx context.Context, target string) NodeView {
	v := NodeView{Target: target}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target+"/status", nil)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	resp, err := p.client.Do(req)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		v.Error = "HTTP " + resp.Status
		return v
	}
	var sb statusBody
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxStatusBytes)).Decode(&sb); err != nil {
		v.Error = "bad status response: " + err.Error()
		return v
	}

	v.Reachable = true
	v.LatencyMS = float64(time.Since(start)) / float64(time.Millisecond)
	v.ID, v.State, v.Term, v.Leader = sb.ID, sb.State, sb.Term, sb.Leader
	v.LastIndex, v.CommitIndex, v.AppliedIndex, v.SnapshotIndex = sb.LastIndex, sb.CommitIndex, sb.AppliedIndex, sb.SnapshotIndex
	v.Ready, v.NodeError = sb.Ready, sb.Error
	return v
}

// summarize computes the cluster summary and marks stale leaders in views.
func summarize(views []NodeView) Summary {
	s := Summary{Total: len(views)}
	for _, v := range views {
		if v.Reachable {
			s.Reachable++
			if v.Term > s.Term {
				s.Term = v.Term
			}
		}
	}
	s.Quorum = s.Reachable*2 > s.Total

	leaders, leader := 0, ""
	for i := range views {
		v := &views[i]
		if !v.Reachable || v.State != "leader" {
			continue
		}
		if v.Term < s.Term {
			v.Stale = true
			continue
		}
		leaders++
		leader = v.label()
	}
	if leaders == 1 { // two leaders in one term would be a bug: show none
		s.Leader = leader
	}
	s.Healthy = s.Quorum && s.Leader != ""
	return s
}

// update turns freshly polled views into a published snapshot: it carries
// node IDs over for unreachable nodes, summarises, records events and wakes
// subscribers.
func (p *Poller) update(now time.Time, views []NodeView) Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range views {
		if !views[i].Reachable && i < len(p.prev) {
			views[i].ID = p.prev[i].ID
		}
	}
	sum := summarize(views)

	var evs []Event
	add := func(kind, format string, args ...any) {
		evs = append(evs, Event{Time: now, Kind: kind, Message: fmt.Sprintf(format, args...)})
	}
	if p.prev == nil {
		add("info", "monitoring %d node(s)", len(views))
	} else {
		for i, v := range views {
			old := p.prev[i]
			switch {
			case old.Reachable && !v.Reachable:
				add("down", "%s is unreachable", v.label())
			case !old.Reachable && v.Reachable:
				add("up", "%s is reachable", v.label())
			case v.Reachable && old.State != v.State:
				add("role", "%s: %s → %s (term %d)", v.label(), old.State, v.State, v.Term)
			}
			if v.Reachable && v.NodeError != "" && old.NodeError == "" {
				add("down", "%s reports a failure: %s", v.label(), v.NodeError)
			}
		}
		if p.prevSummary.Quorum && !sum.Quorum {
			add("warn", "quorum lost: only %d of %d nodes reachable", sum.Reachable, sum.Total)
		} else if !p.prevSummary.Quorum && sum.Quorum {
			add("up", "quorum restored")
		}
	}
	if sum.Leader != p.prevSummary.Leader {
		if sum.Leader != "" {
			add("leader", "leader: %s (term %d)", sum.Leader, sum.Term)
		} else if p.prevSummary.Leader != "" {
			add("warn", "no leader")
		}
	}

	p.events = append(evs, p.events...)
	if len(p.events) > maxEvents {
		p.events = p.events[:maxEvents]
	}
	p.prev = append([]NodeView(nil), views...)
	p.prevSummary = sum

	snap := Snapshot{
		Time:    now,
		Nodes:   views,
		Summary: sum,
		Events:  append([]Event{}, p.events...),
	}
	p.snap, p.has = snap, true
	for ch := range p.subs {
		push(ch, snap)
	}
	return snap
}

// push delivers s to ch, replacing an unread older snapshot. Callers hold p.mu.
func push(ch chan Snapshot, s Snapshot) {
	select {
	case ch <- s:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- s:
	default:
	}
}
