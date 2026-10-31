// Package ops serves a node's operational HTTP endpoints: liveness,
// readiness, a JSON status page and Prometheus metrics.
package ops

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sqmi511/raftkv/internal/metrics"
	"github.com/sqmi511/raftkv/internal/raft"
)

// readiness reports whether the node can take part in serving the cluster:
// its storage is healthy and it knows who the leader is.
func readiness(s raft.Status) (ok bool, reason string) {
	switch {
	case s.Err != nil:
		return false, "node failed: " + s.Err.Error()
	case s.LeaderID == "":
		return false, "no leader known"
	}
	return true, ""
}

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
	Error         string `json:"error,omitempty"`
}

// Handler returns the ops endpoints:
//
//	/healthz  liveness: 200 unless the node's storage has failed (restart it)
//	/readyz   readiness: 200 once a leader is known, 503 otherwise
//	/status   JSON snapshot of the Raft state
//	/metrics  Prometheus metrics from reg
func Handler(status func() raft.Status, reg *metrics.Registry) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s := status(); s.Err != nil {
			http.Error(w, "failed: "+s.Err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ok, reason := readiness(status()); !ok {
			http.Error(w, "not ready: "+reason, http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		s := status()
		ready, _ := readiness(s)
		body := statusBody{
			ID: s.ID, State: s.State.String(), Term: s.Term, Leader: s.LeaderID,
			LastIndex: s.LastIndex, CommitIndex: s.CommitIndex, AppliedIndex: s.Applied,
			SnapshotIndex: s.SnapshotIndex, Ready: ready,
		}
		if s.Err != nil {
			body.Error = s.Err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(body)
	})

	mux.Handle("/metrics", reg.Handler())
	return mux
}

// RegisterNodeMetrics exposes the node's Raft state as gauges on reg.
func RegisterNodeMetrics(reg *metrics.Registry, status func() raft.Status) {
	gauge := func(name, help string, f func(raft.Status) float64) {
		reg.NewGaugeFunc(name, help, func() float64 { return f(status()) })
	}
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}

	gauge("raftkv_raft_term", "Current Raft term.",
		func(s raft.Status) float64 { return float64(s.Term) })
	gauge("raftkv_raft_is_leader", "1 if this node is the leader, else 0.",
		func(s raft.Status) float64 { return b2f(s.State == raft.Leader) })
	gauge("raftkv_raft_leader_known", "1 if this node knows who the leader is, else 0.",
		func(s raft.Status) float64 { return b2f(s.LeaderID != "") })
	gauge("raftkv_raft_last_log_index", "Index of the last log entry.",
		func(s raft.Status) float64 { return float64(s.LastIndex) })
	gauge("raftkv_raft_commit_index", "Highest log index known to be committed.",
		func(s raft.Status) float64 { return float64(s.CommitIndex) })
	gauge("raftkv_raft_applied_index", "Highest log index applied to the state machine.",
		func(s raft.Status) float64 { return float64(s.Applied) })
	gauge("raftkv_raft_snapshot_index", "Last log index covered by the snapshot (0 = none).",
		func(s raft.Status) float64 { return float64(s.SnapshotIndex) })
	gauge("raftkv_raft_failed", "1 if a storage failure took this node out of service.",
		func(s raft.Status) float64 { return b2f(s.Err != nil) })
}
