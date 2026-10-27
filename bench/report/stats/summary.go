package stats

import (
	"cmp"
	"math"
	"slices"
)

// Geo is a geometric mean of relative times across datasets.
type Geo struct {
	Ratio, Lo, Hi float64
	OK            bool
}

// SummaryRow holds the cross-dataset relative time of one format for encode and decode.
type SummaryRow struct {
	Format         string
	Baseline       bool
	Encode, Decode Geo
}

// Summary averages relative encode and decode times over datasets.
// A format is only scored for an op when it was measured on every dataset that has a baseline for it.
func (c Comparison) Summary() []SummaryRow {
	formats := c.formats()
	out := make([]SummaryRow, len(formats))
	for i, f := range formats {
		out[i] = SummaryRow{Format: f, Baseline: f == c.Baseline, Encode: c.geo("encode", f), Decode: c.geo("decode", f)}
	}
	slices.SortFunc(out, compareSummary)
	return out
}

func (c Comparison) formats() []string {
	var all []string
	for _, d := range c.Datasets {
		for _, o := range d.Ops {
			for _, r := range o.Rows {
				all = append(all, r.Format)
			}
		}
	}
	slices.Sort(all)
	return slices.Compact(all)
}

func (c Comparison) geo(op, format string) Geo {
	var ratios, los, his []float64
	for _, d := range c.Datasets {
		i := slices.IndexFunc(d.Ops, func(o Op) bool { return o.Name == op })
		if i < 0 || !d.Ops[i].HasRelativeTime() {
			continue
		}
		j := slices.IndexFunc(d.Ops[i].Rows, func(r Row) bool { return r.Format == format })
		if j < 0 {
			return Geo{}
		}
		rel, ok := d.Ops[i].Rows[j].Time()
		if !ok {
			return Geo{}
		}
		ratios, los, his = append(ratios, rel.Ratio), append(los, rel.Lo), append(his, rel.Hi)
	}
	if len(ratios) == 0 {
		return Geo{}
	}
	return Geo{Ratio: geomean(ratios), Lo: geomean(los), Hi: geomean(his), OK: true}
}

func geomean(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += math.Log(x)
	}
	return math.Exp(sum / float64(len(xs)))
}

func compareSummary(a, b SummaryRow) int {
	if a.Baseline != b.Baseline {
		if a.Baseline {
			return -1
		}
		return 1
	}
	return cmp.Or(cmp.Compare(sortKey(a), sortKey(b)), cmp.Compare(a.Format, b.Format))
}

// sortKey orders by encode ratio, with unscored formats last.
func sortKey(r SummaryRow) float64 {
	if !r.Encode.OK {
		return math.Inf(1)
	}
	return r.Encode.Ratio
}
