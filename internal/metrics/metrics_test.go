package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func gather(r *Registry) string {
	var b strings.Builder
	r.Gather(&b)
	return b.String()
}

func TestCounterVec(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("http_requests_total", "Requests.", "method", "code")
	c.With("GET", "200").Inc()
	c.With("GET", "200").Inc()
	c.With("GET", "500").Inc()
	c.With("POST", "200").Add(3)

	want := `# HELP http_requests_total Requests.
# TYPE http_requests_total counter
http_requests_total{method="GET",code="200"} 2
http_requests_total{method="GET",code="500"} 1
http_requests_total{method="POST",code="200"} 3
`
	if got := gather(r); got != want {
		t.Fatalf("output mismatch.\n got:\n%s\nwant:\n%s", got, want)
	}
	if v := c.With("GET", "200").Value(); v != 2 {
		t.Fatalf("Value = %d, want 2", v)
	}
}

func TestCounterWithoutLabels(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("events_total", "Events.").With().Inc()

	if got := gather(r); !strings.Contains(got, "\nevents_total 1\n") {
		t.Fatalf("output = %q, want a plain 'events_total 1' line", got)
	}
}

func TestGaugeFunc(t *testing.T) {
	r := NewRegistry()
	v := 21.5
	r.NewGaugeFunc("temperature", "Temp.", func() float64 { return v })
	r.NewGaugeFunc("big", "A big whole number.", func() float64 { return 1234567 })

	got := gather(r)
	if !strings.Contains(got, "# TYPE temperature gauge\ntemperature 21.5\n") {
		t.Fatalf("missing temperature gauge in %q", got)
	}
	if !strings.Contains(got, "\nbig 1234567\n") {
		t.Fatalf("whole numbers must not use an exponent: %q", got)
	}
	v = 22 // gauges are read at scrape time
	if got := gather(r); !strings.Contains(got, "temperature 22\n") {
		t.Fatalf("gauge did not follow its function: %q", got)
	}
}

func TestHistogramVec(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogramVec("latency_seconds", "Latency.", []float64{0.25, 1}, "method")
	for _, v := range []float64{0.125, 0.25, 0.5, 2} {
		h.With("a").Observe(v)
	}

	want := `# HELP latency_seconds Latency.
# TYPE latency_seconds histogram
latency_seconds_bucket{method="a",le="0.25"} 2
latency_seconds_bucket{method="a",le="1"} 3
latency_seconds_bucket{method="a",le="+Inf"} 4
latency_seconds_sum{method="a"} 2.875
latency_seconds_count{method="a"} 4
`
	if got := gather(r); got != want {
		t.Fatalf("output mismatch.\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestEscaping(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("esc_total", "Line one\nline two \\ end.", "v").With("a\"b\\c\nd").Inc()

	got := gather(r)
	if !strings.Contains(got, `# HELP esc_total Line one\nline two \\ end.`) {
		t.Fatalf("help text not escaped: %q", got)
	}
	if !strings.Contains(got, `esc_total{v="a\"b\\c\nd"} 1`) {
		t.Fatalf("label value not escaped: %q", got)
	}
}

func TestMetricsAreSortedByName(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("b_metric", "B.", func() float64 { return 1 })
	r.NewGaugeFunc("a_metric", "A.", func() float64 { return 1 })

	got := gather(r)
	if strings.Index(got, "a_metric") > strings.Index(got, "b_metric") {
		t.Fatalf("metrics are not sorted: %q", got)
	}
}

func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.NewGaugeFunc("up", "Up.", func() float64 { return 1 })

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("Content-Type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "up 1\n") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected a panic", name)
		}
	}()
	f()
}

func TestProgrammingErrorsPanic(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("x_total", "X.", "a")

	mustPanic(t, "duplicate name", func() { r.NewCounterVec("x_total", "X.") })
	mustPanic(t, "invalid metric name", func() { r.NewCounterVec("1bad", "X.") })
	mustPanic(t, "invalid label name", func() { r.NewCounterVec("y_total", "Y.", "bad-label") })
	mustPanic(t, "too few label values", func() { c.With() })
	mustPanic(t, "too many label values", func() { c.With("1", "2") })
	mustPanic(t, "unsorted buckets", func() { r.NewHistogramVec("h", "H.", []float64{1, 1}) })
}

func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("c_total", "C.", "k")
	h := r.NewHistogramVec("h_seconds", "H.", DefaultBuckets, "k")

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := string(rune('a' + i%3))
				c.With(k).Inc()
				h.With(k).Observe(float64(i) / 1000)
				if i%100 == 0 {
					gather(r)
				}
			}
		}(g)
	}
	wg.Wait()

	total := c.With("a").Value() + c.With("b").Value() + c.With("c").Value()
	if total != 8*500 {
		t.Fatalf("total = %d, want %d", total, 8*500)
	}
}
