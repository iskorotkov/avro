package stats

import (
	"cmp"
	"maps"
	"math"
	"slices"
)

// Relative is a value normalised to the baseline (1 = 100%).
type Relative struct {
	Ratio, Lo, Hi float64
}

// Row is one format within a dataset and op.
type Row struct {
	Format   string
	Baseline bool
	Est      map[Metric]Estimate
	// Rel is empty when the dataset/op has no usable baseline.
	Rel map[Metric]Relative
}

// Time returns the relative time per op, if known.
func (r Row) Time() (Relative, bool) {
	rel, ok := r.Rel[SecPerRec]
	return rel, ok
}

// Op groups the rows of one operation; the baseline row comes first.
type Op struct {
	Name string
	Rows []Row
}

// HasRelativeTime reports whether any row has a relative time.
func (o Op) HasRelativeTime() bool {
	return slices.ContainsFunc(o.Rows, func(r Row) bool { _, ok := r.Time(); return ok })
}

// Dataset groups the ops measured on one dataset.
type Dataset struct {
	Name string
	Ops  []Op
}

// Comparison is the full normalised report model.
type Comparison struct {
	Env      Env
	Baseline string
	Datasets []Dataset
}

// Compare normalises every dataset/op against the baseline format.
func Compare(data Data, baseline string) Comparison {
	tree := make(map[string]map[string][]Row)
	for key, est := range data.Estimates {
		if tree[key.Dataset] == nil {
			tree[key.Dataset] = make(map[string][]Row)
		}
		tree[key.Dataset][key.Op] = append(tree[key.Dataset][key.Op], Row{Format: key.Format, Baseline: key.Format == baseline, Est: est})
	}

	cmpn := Comparison{Env: data.Env, Baseline: baseline}
	for _, ds := range slices.Sorted(maps.Keys(tree)) {
		d := Dataset{Name: ds}
		for _, op := range slices.SortedFunc(maps.Keys(tree[ds]), compareOps) {
			d.Ops = append(d.Ops, Op{Name: op, Rows: normalise(tree[ds][op])})
		}
		cmpn.Datasets = append(cmpn.Datasets, d)
	}
	return cmpn
}

func normalise(rows []Row) []Row {
	idx := slices.IndexFunc(rows, func(r Row) bool { return r.Baseline })
	if idx >= 0 {
		base := rows[idx].Est
		for i := range rows {
			rows[i].Rel = relativeTo(base, rows[i].Est)
		}
	}
	slices.SortFunc(rows, compareRows)
	return rows
}

func relativeTo(base, est map[Metric]Estimate) map[Metric]Relative {
	rel := make(map[Metric]Relative)
	for _, m := range RelativeMetrics() {
		b, ok1 := base[m]
		e, ok2 := est[m]
		if !ok1 || !ok2 || b.Center == 0 || math.IsNaN(b.Center) {
			continue
		}
		rel[m] = Relative{Ratio: e.Center / b.Center, Lo: e.Lo / b.Center, Hi: e.Hi / b.Center}
	}
	return rel
}

func compareRows(a, b Row) int {
	if a.Baseline != b.Baseline {
		if a.Baseline {
			return -1
		}
		return 1
	}
	ta, oka := a.Est[SecPerRec]
	tb, okb := b.Est[SecPerRec]
	if oka != okb {
		if oka {
			return -1
		}
		return 1
	}
	return cmp.Or(cmp.Compare(ta.Center, tb.Center), cmp.Compare(a.Format, b.Format))
}

func opRank(op string) int {
	switch op {
	case "encode":
		return 0
	case "decode":
		return 1
	case "batch-encode":
		return 2
	case "batch-decode":
		return 3
	}
	return 4
}

func compareOps(a, b string) int {
	return cmp.Or(cmp.Compare(opRank(a), opRank(b)), cmp.Compare(a, b))
}
