package dashboard

import (
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML string

const keepAliveEvery = 15 * time.Second

// Server serves the dashboard page, a JSON endpoint and a live event stream.
type Server struct {
	p         *Poller
	closing   chan struct{}
	closeOnce sync.Once
}

// NewServer returns a Server that presents p's snapshots.
func NewServer(p *Poller) *Server {
	return &Server{p: p, closing: make(chan struct{})}
}

// Close ends every open event stream so the HTTP server can shut down. It is
// idempotent.
func (s *Server) Close() { s.closeOnce.Do(func() { close(s.closing) }) }

// Handler returns the HTTP handler:
//
//	/           the dashboard page
//	/events     live snapshots (Server-Sent Events)
//	/api/state  the latest snapshot as JSON
//	/healthz    liveness of the dashboard itself
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", getOnly(s.index))
	mux.HandleFunc("/events", getOnly(s.events))
	mux.HandleFunc("/api/state", getOnly(s.state))
	mux.HandleFunc("/healthz", getOnly(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	}))
	return mux
}

func getOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	nonce := base64.StdEncoding.EncodeToString(raw[:])

	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Content-Security-Policy", "default-src 'none'; script-src 'nonce-"+nonce+"'; style-src 'nonce-"+nonce+
		"'; connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	_, _ = w.Write([]byte(strings.ReplaceAll(indexHTML, "{{NONCE}}", nonce)))
}

// latest returns the newest snapshot, or an empty one before the first poll.
func (s *Server) latest() Snapshot {
	if snap, ok := s.p.Snapshot(); ok {
		return snap
	}
	return Snapshot{Nodes: []NodeView{}, Events: []Event{}}
}

func (s *Server) state(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(s.latest())
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(": connected\n\n")); err != nil {
		return
	}
	fl.Flush()

	snaps, cancel := s.p.Subscribe()
	defer cancel()
	keepAlive := time.NewTicker(keepAliveEvery)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.closing:
			return
		case snap := <-snaps:
			b, err := json.Marshal(snap)
			if err != nil {
				return
			}
			if _, err := w.Write(append(append([]byte("data: "), b...), '\n', '\n')); err != nil {
				return
			}
			fl.Flush()
		case <-keepAlive.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			fl.Flush()
		}
	}
}
