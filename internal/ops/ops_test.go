package ops

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sqmi511/raftkv/internal/metrics"
	"github.com/sqmi511/raftkv/internal/raft"
)

func fixed(s raft.Status) func() raft.Status { return func() raft.Status { return s } }

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

var leaderStatus = raft.Status{
	ID: "a:1", State: raft.Leader, Term: 7, LeaderID: "a:1",
	LastIndex: 12, CommitIndex: 11, Applied: 10, SnapshotIndex: 5,
}

func TestHealthAndReadiness(t *testing.T) {
	cases := []struct {
		name         string
		status       raft.Status
		health, read int
		readBody     string
	}{
		{"leader", leaderStatus, 200, 200, ""},
		{"follower with a known leader", raft.Status{State: raft.Follower, LeaderID: "b:1"}, 200, 200, ""},
		{"no leader known", raft.Status{State: raft.Candidate}, 200, 503, "no leader"},
		{"storage failed", raft.Status{LeaderID: "a:1", Err: errors.New("disk on fire")}, 503, 503, "disk on fire"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := Handler(fixed(tc.status), metrics.NewRegistry())

			if rec := get(h, "/healthz"); rec.Code != tc.health {
				t.Errorf("/healthz = %d, want %d", rec.Code, tc.health)
			}
			rec := get(h, "/readyz")
			if rec.Code != tc.read {
				t.Errorf("/readyz = %d, want %d", rec.Code, tc.read)
			}
			if tc.readBody != "" && !strings.Contains(rec.Body.String(), tc.readBody) {
				t.Errorf("/readyz body = %q, want it to mention %q", rec.Body.String(), tc.readBody)
			}
		})
	}
}

func TestStatusEndpoint(t *testing.T) {
	h := Handler(fixed(leaderStatus), metrics.NewRegistry())

	rec := get(h, "/status")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, rec.Body.String())
	}
	if got["state"] != "leader" || got["leader"] != "a:1" || got["term"] != float64(7) ||
		got["commit_index"] != float64(11) || got["snapshot_index"] != float64(5) || got["ready"] != true {
		t.Fatalf("status = %v", got)
	}
	if _, present := got["error"]; present {
		t.Fatal("a healthy node must not report an error field")
	}

	failed := Handler(fixed(raft.Status{Err: errors.New("boom")}), metrics.NewRegistry())
	var f map[string]any
	if err := json.Unmarshal(get(failed, "/status").Body.Bytes(), &f); err != nil {
		t.Fatal(err)
	}
	if f["error"] != "boom" || f["ready"] != false {
		t.Fatalf("failed status = %v", f)
	}
}

func TestNodeMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	RegisterNodeMetrics(reg, fixed(leaderStatus))

	body := get(Handler(fixed(leaderStatus), reg), "/metrics").Body.String()
	for _, want := range []string{
		"raftkv_raft_term 7\n",
		"raftkv_raft_is_leader 1\n",
		"raftkv_raft_leader_known 1\n",
		"raftkv_raft_last_log_index 12\n",
		"raftkv_raft_commit_index 11\n",
		"raftkv_raft_applied_index 10\n",
		"raftkv_raft_snapshot_index 5\n",
		"raftkv_raft_failed 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q", want)
		}
	}
}
