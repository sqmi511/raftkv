// Package bench measures raftkv: throughput and latency under load, and how
// long a client is without service after the leader crashes.
package bench

import (
	"sort"
	"time"
)

// Summary describes a set of latency samples.
type Summary struct {
	Count int
	Min   time.Duration
	Mean  time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

// Summarize computes a Summary. The input is not modified. An empty input
// gives the zero Summary.
func Summarize(samples []time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	s := append([]time.Duration(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	var sum int64
	for _, d := range s {
		sum += int64(d)
	}
	return Summary{
		Count: len(s),
		Min:   s[0],
		Mean:  time.Duration(sum / int64(len(s))),
		P50:   percentile(s, 50),
		P95:   percentile(s, 95),
		P99:   percentile(s, 99),
		Max:   s[len(s)-1],
	}
}

// percentile returns the nearest-rank percentile p (1..100) of sorted: the
// smallest sample such that at least p percent of samples are <= it. Integer
// arithmetic avoids floating-point rounding at exact boundaries.
func percentile(sorted []time.Duration, p int) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := (n*p + 99) / 100 // ceil(n*p/100)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}
