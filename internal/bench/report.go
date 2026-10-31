package bench

import (
	"fmt"
	"strings"
	"time"
)

// Report is everything a benchmark run produced.
type Report struct {
	Env      []string // one line per fact about the setup
	Load     []Result
	Failover []FailoverTrial
}

func ms(d time.Duration, prec int) string {
	return fmt.Sprintf("%.*f", prec, float64(d)/float64(time.Millisecond))
}

// Markdown renders the report as GitHub-flavoured Markdown, ready to paste
// into the README.
func (r Report) Markdown() string {
	var b strings.Builder

	if len(r.Env) > 0 {
		b.WriteString("**Setup**\n\n")
		for _, line := range r.Env {
			b.WriteString("- " + line + "\n")
		}
		b.WriteString("\n")
	}

	if len(r.Load) > 0 {
		b.WriteString("**Throughput and latency**\n\n")
		b.WriteString("| Workload | Clients | Ops/sec | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, res := range r.Load {
			l := res.Latency
			fmt.Fprintf(&b, "| %s | %d | %.0f | %s | %s | %s | %s | %d |\n",
				res.Workload.Name, res.Workload.Clients, res.OpsPerSec(),
				ms(l.P50, 2), ms(l.P95, 2), ms(l.P99, 2), ms(l.Max, 2), res.Errors)
		}
		b.WriteString("\n")
	}

	if len(r.Failover) > 0 {
		var election, write []time.Duration
		for _, t := range r.Failover {
			election = append(election, t.Election)
			write = append(write, t.Write)
		}
		es, ws := Summarize(election), Summarize(write)

		b.WriteString("**Failover** (leader crashed; time counted from the crash)\n\n")
		b.WriteString("| Trial | New leader elected (ms) | First successful write (ms) |\n")
		b.WriteString("|---|---:|---:|\n")
		for i, t := range r.Failover {
			fmt.Fprintf(&b, "| %d | %s | %s |\n", i+1, ms(t.Election, 1), ms(t.Write, 1))
		}
		fmt.Fprintf(&b, "| **median** | %s | %s |\n", ms(es.P50, 1), ms(ws.P50, 1))
		fmt.Fprintf(&b, "| **max** | %s | %s |\n", ms(es.Max, 1), ms(ws.Max, 1))
		b.WriteString("\n")
	}
	return b.String()
}
