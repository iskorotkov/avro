package markdown

import (
	"fmt"
	"math"

	"github.com/iskorotkov/avro/bench/report/stats"
	"golang.org/x/perf/benchunit"
)

const missing = "—"

func seconds(v float64) string  { return withUnit(v, benchunit.Decimal, "s") }
func bytes(v float64) string    { return withUnit(v, benchunit.Binary, "B") }
func byteRate(v float64) string { return withUnit(v, benchunit.Decimal, "B/s") }
func number(v float64) string   { return withUnit(v, benchunit.Decimal, "") }

func withUnit(v float64, class benchunit.Class, unit string) string {
	if v == 0 {
		return "0" + unit
	}
	return benchunit.Scale(v, class) + unit
}

// absolute renders a metric's median with its spread, or a dash when absent.
func absolute(r stats.Row, m stats.Metric, format func(float64) string) string {
	e, ok := r.Est[m]
	if !ok {
		return missing
	}
	return format(e.Center) + spread(e)
}

func spread(e stats.Estimate) string {
	s := ""
	switch sp := e.Spread(); {
	case math.IsInf(sp, 1):
		s = " ±∞"
	case sp > 0:
		s = " ±" + percent(sp)
	}
	if e.Noisy {
		s += " ⚠"
	}
	return s
}

// relative renders a metric as a percentage of the baseline, or a dash without one.
func relative(r stats.Row, m stats.Metric) string {
	rel, ok := r.Rel[m]
	if !ok {
		return missing
	}
	return percent(rel.Ratio)
}

func percent(ratio float64) string {
	pct := ratio * 100
	if math.Abs(pct) < 10 {
		return fmt.Sprintf("%.1f%%", pct)
	}
	return fmt.Sprintf("%.0f%%", pct)
}
