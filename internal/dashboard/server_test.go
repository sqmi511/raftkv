package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*Server, *Poller) {
	t.Helper()
	node := fakeNode(t, "a", "leader", 3, "a")
	p := newPoller(t, node.URL)
	p.PollOnce(context.Background())
	s := NewServer(p)
	t.Cleanup(s.Close)
	return s, p
}

func serve(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

var nonceRe = regexp.MustCompile(`script-src 'nonce-([^']+)'`)

func TestIndexPageAndContentSecurityPolicy(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()

	rec := serve(h, http.MethodGet, "/")
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("GET / = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	if strings.Contains(body, "{{NONCE}}") || !strings.Contains(body, "<title>raftkv dashboard</title>") {
		t.Fatal("the page was not rendered correctly")
	}

	csp := rec.Header().Get("Content-Security-Policy")
	m := nonceRe.FindStringSubmatch(csp)
	if m == nil || !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "connect-src 'self'") {
		t.Fatalf("CSP = %q", csp)
	}
	if !strings.Contains(body, `<script nonce="`+m[1]+`">`) || !strings.Contains(body, `<style nonce="`+m[1]+`">`) {
		t.Fatal("the CSP nonce does not match the nonce in the page")
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("missing security headers: %v", rec.Header())
	}

	again := nonceRe.FindStringSubmatch(serve(h, http.MethodGet, "/").Header().Get("Content-Security-Policy"))
	if again == nil || again[1] == m[1] {
		t.Fatal("the nonce must be different on every request")
	}
}

func TestRoutingAndMethods(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()

	if rec := serve(h, http.MethodGet, "/nope"); rec.Code != 404 {
		t.Fatalf("GET /nope = %d, want 404", rec.Code)
	}
	if rec := serve(h, http.MethodGet, "/healthz"); rec.Code != 200 {
		t.Fatalf("GET /healthz = %d", rec.Code)
	}
	for _, path := range []string{"/", "/events", "/api/state", "/healthz"} {
		rec := serve(h, http.MethodPost, path)
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET" {
			t.Errorf("POST %s = %d (Allow %q), want 405", path, rec.Code, rec.Header().Get("Allow"))
		}
	}
}

func TestStateEndpoint(t *testing.T) {
	s, _ := newTestServer(t)
	rec := serve(s.Handler(), http.MethodGet, "/api/state")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("GET /api/state = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var snap Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Summary.Leader != "a" || len(snap.Nodes) != 1 || !snap.Nodes[0].Reachable {
		t.Fatalf("snapshot = %+v", snap)
	}

	// Before the first poll the lists are empty arrays, not null: the page iterates them.
	empty := NewServer(newPoller(t, "http://127.0.0.1:1"))
	body := serve(empty.Handler(), http.MethodGet, "/api/state").Body.String()
	if !strings.Contains(body, `"nodes":[]`) || !strings.Contains(body, `"events":[]`) {
		t.Fatalf("empty state = %s", body)
	}
}

func TestEventStreamDeliversSnapshotsAndEndsOnClose(t *testing.T) {
	s, p := newTestServer(t)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close) // runs after s.Close, which ends the stream

	resp, err := http.Get(ts.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("GET /events = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	lines := make(chan string, 16)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	nextData := func() Snapshot {
		t.Helper()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatal("stream ended early")
				}
				if payload, found := strings.CutPrefix(line, "data: "); found {
					var snap Snapshot
					if err := json.Unmarshal([]byte(payload), &snap); err != nil {
						t.Fatalf("bad event payload: %v", err)
					}
					return snap
				}
			case <-timeout:
				t.Fatal("no event received")
			}
		}
	}()

	if nextData.Summary.Leader != "a" {
		t.Fatalf("first event = %+v", nextData)
	}
	p.PollOnce(context.Background())
	if nextData2 := func() Snapshot {
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatal("stream ended early")
				}
				if payload, found := strings.CutPrefix(line, "data: "); found {
					var snap Snapshot
					_ = json.Unmarshal([]byte(payload), &snap)
					return snap
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no live update received")
			}
		}
	}(); !nextData2.Time.After(nextData.Time) {
		t.Fatal("the live update is not newer than the first event")
	}

	s.Close()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-lines:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the stream did not end after Server.Close")
		}
	}
}
