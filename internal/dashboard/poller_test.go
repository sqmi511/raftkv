package dashboard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeNode serves a fixed /status document, like a raftkv node's ops endpoint.
func fakeNode(t *testing.T, id, state string, term uint64, leader string) *httptest.Server {
	t.Helper()
	body := fmt.Sprintf(`{"id":%q,"state":%q,"term":%d,"leader":%q,"last_index":10,"commit_index":9,`+
		`"applied_index":8,"snapshot_index":5,"ready":true}`, id, state, term, leader)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newPoller(t *testing.T, targets ...string) *Poller {
	t.Helper()
	p, err := NewPoller(Options{Targets: targets, Timeout: 2 * time.Second, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func up(id, state string, term uint64) NodeView {
	return NodeView{Target: "http://" + id, ID: id, Reachable: true, State: state, Term: term}
}

func down(id string) NodeView { return NodeView{Target: "http://" + id, Error: "refused"} }

func eventMessages(s Snapshot) string {
	var parts []string
	for _, e := range s.Events {
		parts = append(parts, e.Kind+": "+e.Message)
	}
	return strings.Join(parts, "\n")
}

func TestPollOnceSummarisesACluster(t *testing.T) {
	a := fakeNode(t, "a", "leader", 3, "a")
	b := fakeNode(t, "b", "follower", 3, "a")
	c := fakeNode(t, "c", "follower", 3, "a")
	p := newPoller(t, a.URL, b.URL, c.URL)

	if _, ok := p.Snapshot(); ok {
		t.Fatal("a snapshot exists before the first poll")
	}
	snap := p.PollOnce(context.Background())

	want := Summary{Total: 3, Reachable: 3, Quorum: true, Leader: "a", Term: 3, Healthy: true}
	if snap.Summary != want {
		t.Fatalf("summary = %+v, want %+v", snap.Summary, want)
	}
	n := snap.Nodes[0]
	if !n.Reachable || n.State != "leader" || n.CommitIndex != 9 || n.LastIndex != 10 ||
		n.AppliedIndex != 8 || n.SnapshotIndex != 5 || !n.Ready || n.LatencyMS < 0 || n.Target != a.URL {
		t.Fatalf("node view = %+v", n)
	}
	if msg := eventMessages(snap); !strings.Contains(msg, "monitoring 3") || !strings.Contains(msg, "leader: a (term 3)") {
		t.Fatalf("events:\n%s", msg)
	}
	if latest, ok := p.Snapshot(); !ok || !latest.Time.Equal(snap.Time) {
		t.Fatal("Snapshot() does not return the published snapshot")
	}
}

func TestUnreachableNodeKeepsItsIDAndRaisesAnEvent(t *testing.T) {
	a := fakeNode(t, "a", "leader", 3, "a")
	b := fakeNode(t, "b", "follower", 3, "a")
	c := fakeNode(t, "c", "follower", 3, "a")
	p := newPoller(t, a.URL, b.URL, c.URL)
	p.PollOnce(context.Background())

	b.Close()
	snap := p.PollOnce(context.Background())

	if n := snap.Nodes[1]; n.Reachable || n.Error == "" || n.ID != "b" {
		t.Fatalf("unreachable node view = %+v, want Reachable=false, an error and the remembered ID", n)
	}
	if !snap.Summary.Quorum || !snap.Summary.Healthy || snap.Summary.Reachable != 2 {
		t.Fatalf("summary = %+v, want a healthy 2-of-3 cluster", snap.Summary)
	}
	if first := snap.Events[0]; first.Kind != "down" || first.Message != "b is unreachable" {
		t.Fatalf("newest event = %+v", first)
	}
}

func TestBadResponsesMarkTheNodeUnreachable(t *testing.T) {
	handlers := map[string]http.HandlerFunc{
		"server error": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", 500) },
		"not json":     func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "<html>") },
		"oversized": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat(" ", 2<<20)+"{}")
		},
		"redirect": func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) },
	}
	for name, h := range handlers {
		srv := httptest.NewServer(h)
		snap := newPoller(t, srv.URL).PollOnce(context.Background())
		srv.Close()
		if n := snap.Nodes[0]; n.Reachable || n.Error == "" {
			t.Errorf("%s: node view = %+v, want unreachable with an error", name, n)
		}
	}
}

func TestStaleLeaderIsFlagged(t *testing.T) {
	p := newPoller(t, "x")
	snap := p.update(time.Now(), []NodeView{up("a", "leader", 2), up("b", "leader", 3), up("c", "follower", 3)})

	if !snap.Nodes[0].Stale || snap.Nodes[1].Stale {
		t.Fatalf("stale flags = %v %v, want the term-2 leader flagged only", snap.Nodes[0].Stale, snap.Nodes[1].Stale)
	}
	if snap.Summary.Leader != "b" || !snap.Summary.Healthy {
		t.Fatalf("summary = %+v, want leader b", snap.Summary)
	}
}

func TestSummaryEdgeCases(t *testing.T) {
	p := newPoller(t, "x")

	noLeader := p.update(time.Now(), []NodeView{up("a", "follower", 1), up("b", "candidate", 2), up("c", "follower", 1)})
	if s := noLeader.Summary; !s.Quorum || s.Leader != "" || s.Healthy {
		t.Fatalf("no leader: %+v", s)
	}

	two := newPoller(t, "x").update(time.Now(), []NodeView{up("a", "leader", 4), up("b", "leader", 4), up("c", "follower", 4)})
	if two.Summary.Leader != "" || two.Summary.Healthy {
		t.Fatalf("two leaders in one term must not be reported as a healthy cluster: %+v", two.Summary)
	}

	lost := newPoller(t, "x").update(time.Now(), []NodeView{up("a", "leader", 4), down("b"), down("c")})
	if s := lost.Summary; s.Quorum || s.Healthy || s.Reachable != 1 {
		t.Fatalf("minority: %+v", s)
	}
}

func TestEventsForQuorumRoleAndLeaderChanges(t *testing.T) {
	p := newPoller(t, "x")
	now := time.Now()
	p.update(now, []NodeView{up("a", "leader", 1), up("b", "follower", 1), up("c", "follower", 1)})

	snap := p.update(now, []NodeView{down("a"), down("b"), up("c", "follower", 1)})
	msg := eventMessages(snap)
	for _, want := range []string{"a is unreachable", "b is unreachable", "quorum lost: only 1 of 3", "no leader"} {
		if !strings.Contains(msg, want) {
			t.Errorf("events are missing %q:\n%s", want, msg)
		}
	}

	snap = p.update(now, []NodeView{down("a"), up("b", "candidate", 2), up("c", "follower", 1)})
	msg = eventMessages(snap)
	for _, want := range []string{"b is reachable", "quorum restored"} {
		if !strings.Contains(msg, want) {
			t.Errorf("events are missing %q:\n%s", want, msg)
		}
	}

	snap = p.update(now, []NodeView{down("a"), up("b", "leader", 2), up("c", "follower", 2)})
	msg = eventMessages(snap)
	for _, want := range []string{"b: candidate → leader (term 2)", "leader: b (term 2)"} {
		if !strings.Contains(msg, want) {
			t.Errorf("events are missing %q:\n%s", want, msg)
		}
	}

	failed := up("b", "leader", 2)
	failed.NodeError = "disk on fire"
	snap = p.update(now, []NodeView{down("a"), failed, up("c", "follower", 2)})
	if !strings.Contains(eventMessages(snap), "b reports a failure: disk on fire") {
		t.Fatalf("events:\n%s", eventMessages(snap))
	}
}

func TestEventLogIsBoundedAndNewestFirst(t *testing.T) {
	p := newPoller(t, "x")
	var snap Snapshot
	for i := 0; i < 60; i++ {
		state := "leader"
		if i%2 == 1 {
			state = "follower"
		}
		snap = p.update(time.Now(), []NodeView{up("a", state, uint64(i))})
	}
	if len(snap.Events) != maxEvents {
		t.Fatalf("got %d events, want the log capped at %d", len(snap.Events), maxEvents)
	}
	if !strings.Contains(snap.Events[0].Message, "term 59") {
		t.Fatalf("newest event is %q, want the one for term 59", snap.Events[0].Message)
	}
}

func TestParseTargets(t *testing.T) {
	got, err := ParseTargets(" 127.0.0.1:8001, http://node2:8000/ ,https://n3:1,127.0.0.1:8001,, ")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"http://127.0.0.1:8001", "http://node2:8000", "https://n3:1"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	for _, bad := range []string{"", " , ", "ftp://x:1", "http://", "http://x:1/status", "http://x:1?q=1", "http://u:p@x:1", "http://x:1#f", "http://[::1"} {
		if _, err := ParseTargets(bad); !errors.Is(err, ErrInvalidTarget) {
			t.Errorf("ParseTargets(%q) err = %v, want ErrInvalidTarget", bad, err)
		}
	}
	if _, err := NewPoller(Options{}); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("NewPoller without targets = %v, want ErrInvalidTarget", err)
	}
}

func TestSubscribeDeliversAndSkipsToTheNewest(t *testing.T) {
	a := fakeNode(t, "a", "leader", 1, "a")
	p := newPoller(t, a.URL)

	snaps, cancel := p.Subscribe()
	select {
	case <-snaps:
		t.Fatal("received a snapshot before any poll")
	default:
	}

	p.PollOnce(context.Background())
	last := p.PollOnce(context.Background()) // the reader has not read the first one
	select {
	case got := <-snaps:
		if !got.Time.Equal(last.Time) {
			t.Fatal("a slow subscriber got an old snapshot instead of the newest")
		}
	case <-time.After(time.Second):
		t.Fatal("no snapshot delivered")
	}

	late, lateCancel := p.Subscribe() // a new subscriber gets the current state at once
	defer lateCancel()
	select {
	case got := <-late:
		if !got.Time.Equal(last.Time) {
			t.Fatal("late subscriber got the wrong snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("late subscriber got nothing")
	}

	cancel()
	p.PollOnce(context.Background())
	select {
	case <-snaps:
		t.Fatal("a cancelled subscription still receives snapshots")
	default:
	}
}

func TestRunPollsUntilCancelled(t *testing.T) {
	a := fakeNode(t, "a", "leader", 1, "a")
	p := newPoller(t, a.URL)
	snaps, cancelSub := p.Subscribe()
	defer cancelSub()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-snaps:
		case <-time.After(5 * time.Second):
			t.Fatal("Run stopped publishing snapshots")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}
