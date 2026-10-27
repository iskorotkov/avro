// Package stats summarises benchmark samples and compares formats against a baseline.
package stats

import "math"

// Metric is a tidied benchfmt unit that the report understands.
type Metric string

// Metrics the report reads from raw results.
const (
	SecPerOp    Metric = "sec/op"
	SecPerRec   Metric = "sec/rec"
	BytesPerOp  Metric = "B/op"
	AllocsPerOp Metric = "allocs/op"
	WirePerRec  Metric = "wire-B/rec"
	RecPerSec   Metric = "rec/s"
	BytesPerSec Metric = "B/s"
)

// AllMetrics lists every metric read from raw results.
func AllMetrics() []Metric {
	return []Metric{SecPerOp, SecPerRec, BytesPerOp, AllocsPerOp, WirePerRec, RecPerSec, BytesPerSec}
}

// RelativeMetrics lists the metrics that are normalised against the baseline; time is per record so units of work may differ.
func RelativeMetrics() []Metric {
	return []Metric{SecPerRec, BytesPerOp, AllocsPerOp, WirePerRec}
}

// Key identifies one benchmark series.
type Key struct {
	Dataset, Op, Format string
}

// Estimate is a median with its confidence interval.
type Estimate struct {
	Center, Lo, Hi float64
	// Noisy marks a wide interval or a benchmath warning such as too few samples.
	Noisy bool
}

// Spread is the CI half-width relative to the center, or +Inf when unbounded.
func (e Estimate) Spread() float64 {
	if math.IsInf(e.Lo, 0) || math.IsInf(e.Hi, 0) || math.IsNaN(e.Lo) || math.IsNaN(e.Hi) {
		return math.Inf(1)
	}
	half := (e.Hi - e.Lo) / 2
	switch {
	case half == 0:
		return 0
	case e.Center == 0:
		return math.Inf(1)
	}
	return math.Abs(half / e.Center)
}

// Env describes where and how the raw results were produced.
type Env struct {
	CPU, GOOS, GOARCH, Go, Commit, Date, GOMAXPROCS, Flags string
}

// Data holds per-series estimates for every metric.
type Data struct {
	Env       Env
	Estimates map[Key]map[Metric]Estimate
}
