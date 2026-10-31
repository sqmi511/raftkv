// Package metrics is a small, dependency-free implementation of the parts of
// Prometheus instrumentation that raftkv needs: labelled counters, labelled
// histograms and gauges read from a function. A Registry renders them in the
// text exposition format (version 0.0.4) that Prometheus scrapes.
//
// Programming mistakes (invalid or duplicate metric names, wrong number of
// label values) panic, like the official client library.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type collector interface {
	write(w io.Writer)
}

type meta struct{ name, help, typ string }

func (m meta) header(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", m.name, helpEscaper.Replace(m.help), m.name, m.typ)
}

// Registry holds metrics and renders them. It is safe for concurrent use.
type Registry struct {
	mu   sync.Mutex
	cols map[string]collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{cols: make(map[string]collector)}
}

func (r *Registry) register(name string, labels []string, c collector) {
	if !validName(name, true) {
		panic("metrics: invalid metric name " + strconv.Quote(name))
	}
	for _, l := range labels {
		if !validName(l, false) {
			panic("metrics: invalid label name " + strconv.Quote(l))
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.cols[name]; dup {
		panic("metrics: duplicate metric " + name)
	}
	r.cols[name] = c
}

// Gather writes every metric, sorted by name, in the Prometheus text format.
func (r *Registry) Gather(w io.Writer) {
	r.mu.Lock()
	names := make([]string, 0, len(r.cols))
	for n := range r.cols {
		names = append(names, n)
	}
	sort.Strings(names)
	cols := make([]collector, len(names))
	for i, n := range names {
		cols[i] = r.cols[n]
	}
	r.mu.Unlock()

	for _, c := range cols {
		c.write(w)
	}
}

// Handler serves the registry for Prometheus to scrape.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Gather(w)
	})
}

// ---- gauge ------------------------------------------------------------------

type gaugeFunc struct {
	meta
	fn func() float64
}

// NewGaugeFunc registers a gauge whose value is read from fn at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.register(name, nil, &gaugeFunc{meta: meta{name, help, "gauge"}, fn: fn})
}

func (g *gaugeFunc) write(w io.Writer) {
	g.header(w)
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.fn()))
}

// ---- counter ----------------------------------------------------------------

// Counter is a value that only goes up.
type Counter struct {
	values []string
	n      atomic.Uint64
}

// Inc adds one.
func (c *Counter) Inc() { c.n.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.n.Add(n) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.n.Load() }

// CounterVec is a family of counters distinguished by label values.
type CounterVec struct {
	meta
	labels []string

	mu       sync.Mutex
	children map[string]*Counter
}

// NewCounterVec registers a counter family. Use no labels for a plain counter.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{
		meta:     meta{name, help, "counter"},
		labels:   append([]string(nil), labels...),
		children: make(map[string]*Counter),
	}
	r.register(name, labels, v)
	return v
}

// With returns the counter for the given label values, creating it if needed.
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s needs %d label values, got %d", v.name, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\xff")

	v.mu.Lock()
	defer v.mu.Unlock()
	c, ok := v.children[key]
	if !ok {
		c = &Counter{values: append([]string(nil), values...)}
		v.children[key] = c
	}
	return c
}

func (v *CounterVec) write(w io.Writer) {
	v.header(w)

	v.mu.Lock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([]*Counter, len(keys))
	for i, k := range keys {
		rows[i] = v.children[k]
	}
	v.mu.Unlock()

	for _, c := range rows {
		fmt.Fprintf(w, "%s%s %d\n", v.name, labelString(v.labels, c.values, "", ""), c.n.Load())
	}
}

// ---- histogram --------------------------------------------------------------

// Histogram counts observations into buckets.
type Histogram struct {
	values []string
	bounds []float64

	mu     sync.Mutex
	counts []uint64 // per bucket, NOT cumulative; the last slot is +Inf
	sum    float64
	count  uint64
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bucket with bound >= v
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.count++
	h.mu.Unlock()
}

func (h *Histogram) snapshot() (counts []uint64, sum float64, count uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.counts...), h.sum, h.count
}

// HistogramVec is a family of histograms distinguished by label values.
type HistogramVec struct {
	meta
	labels  []string
	buckets []float64

	mu       sync.Mutex
	children map[string]*Histogram
}

// DefaultBuckets suits request latencies, in seconds.
var DefaultBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// NewHistogramVec registers a histogram family. buckets must be strictly
// increasing upper bounds; the +Inf bucket is added automatically.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	for i, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= buckets[i-1]) {
			panic("metrics: histogram buckets must be finite and strictly increasing")
		}
	}
	v := &HistogramVec{
		meta:     meta{name, help, "histogram"},
		labels:   append([]string(nil), labels...),
		buckets:  append([]float64(nil), buckets...),
		children: make(map[string]*Histogram),
	}
	r.register(name, append(append([]string(nil), labels...), "le"), v)
	return v
}

// With returns the histogram for the given label values, creating it if needed.
func (v *HistogramVec) With(values ...string) *Histogram {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s needs %d label values, got %d", v.name, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\xff")

	v.mu.Lock()
	defer v.mu.Unlock()
	h, ok := v.children[key]
	if !ok {
		h = &Histogram{
			values: append([]string(nil), values...),
			bounds: v.buckets,
			counts: make([]uint64, len(v.buckets)+1),
		}
		v.children[key] = h
	}
	return h
}

func (v *HistogramVec) write(w io.Writer) {
	v.header(w)

	v.mu.Lock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([]*Histogram, len(keys))
	for i, k := range keys {
		rows[i] = v.children[k]
	}
	v.mu.Unlock()

	for _, h := range rows {
		counts, sum, count := h.snapshot()
		var cum uint64
		for i, b := range v.buckets {
			cum += counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", v.name, labelString(v.labels, h.values, "le", formatFloat(b)), cum)
		}
		cum += counts[len(v.buckets)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", v.name, labelString(v.labels, h.values, "le", "+Inf"), cum)
		fmt.Fprintf(w, "%s_sum%s %s\n", v.name, labelString(v.labels, h.values, "", ""), formatFloat(sum))
		fmt.Fprintf(w, "%s_count%s %d\n", v.name, labelString(v.labels, h.values, "", ""), count)
	}
}

// ---- formatting helpers -------------------------------------------------------

var (
	labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

func validName(s string, allowColon bool) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c == ':' && allowColon:
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// labelString renders {a="x",b="y"} plus an optional extra label (used for
// histogram buckets). It returns "" when there is nothing to render.
func labelString(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(labelEscaper.Replace(values[i]))
		b.WriteByte('"')
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName)
		b.WriteString(`="`)
		b.WriteString(extraValue)
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// formatFloat prints whole numbers without an exponent (1000000, not 1e+06).
func formatFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
