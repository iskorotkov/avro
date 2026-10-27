package stats_test

import (
	"math"
	"testing"

	"github.com/iskorotkov/avro/bench/report/stats"
)

func est(c float64) map[stats.Metric]stats.Estimate {
	return map[stats.Metric]stats.Estimate{stats.SecPerRec: {Center: c, Lo: c * 0.9, Hi: c * 1.1}}
}

func testData() stats.Data {
	d := stats.Data{Estimates: map[stats.Key]map[stats.Metric]stats.Estimate{}}
	for _, k := range []struct {
		ds, op, f string
		v         float64
	}{
		{"a", "decode", "avro", 10},
		{"a", "decode", "x", 20},
		{"a", "encode", "avro", 10},
		{"a", "encode", "x", 5},
		{"a", "encode", "y", 2},
		{"b", "encode", "avro", 100},
		{"b", "encode", "x", 20},
		{"b", "container-write-zstd", "x", 1},
		{"b", "batch-encode", "avro", 1},
	} {
		d.Estimates[stats.Key{Dataset: k.ds, Op: k.op, Format: k.f}] = est(k.v)
	}
	return d
}

func TestCompareOrdering(t *testing.T) {
	c := stats.Compare(testData(), "avro")
	if len(c.Datasets) != 2 || c.Datasets[0].Name != "a" {
		t.Fatalf("datasets = %+v", c.Datasets)
	}
	var ops []string
	for _, o := range c.Datasets[1].Ops {
		ops = append(ops, o.Name)
	}
	if want := []string{"encode", "batch-encode", "container-write-zstd"}; !equal(ops, want) {
		t.Errorf("ops = %v, want %v", ops, want)
	}
	var formats []string
	for _, r := range c.Datasets[0].Ops[0].Rows {
		formats = append(formats, r.Format)
	}
	if want := []string{"avro", "y", "x"}; !equal(formats, want) {
		t.Errorf("rows = %v, want %v", formats, want)
	}
}

func TestCompareRelative(t *testing.T) {
	c := stats.Compare(testData(), "avro")
	row := c.Datasets[0].Ops[1].Rows[1]
	rel, ok := row.Time()
	if !ok || rel.Ratio != 2 || math.Abs(rel.Hi-2.2) > 1e-9 {
		t.Errorf("relative = %+v ok=%v", rel, ok)
	}
	noBase := c.Datasets[1].Ops[2]
	if noBase.HasRelativeTime() {
		t.Error("op without baseline must have no relative time")
	}
}

func TestSummaryGeomean(t *testing.T) {
	var x stats.SummaryRow
	for _, r := range stats.Compare(testData(), "avro").Summary() {
		if r.Format == "x" {
			x = r
		}
	}
	if want := math.Sqrt(0.5 * 0.2); !x.Encode.OK || math.Abs(x.Encode.Ratio-want) > 1e-9 {
		t.Errorf("encode = %+v, want %v", x.Encode, want)
	}
	if !x.Decode.OK || x.Decode.Ratio != 2 {
		t.Errorf("decode = %+v", x.Decode)
	}
	for _, r := range stats.Compare(testData(), "avro").Summary() {
		if r.Format == "y" && r.Encode.OK {
			t.Error("format missing from a dataset must not be scored")
		}
	}
}

func TestSpread(t *testing.T) {
	if got := (stats.Estimate{Center: 10, Lo: 9, Hi: 11}).Spread(); got != 0.1 {
		t.Errorf("spread = %v", got)
	}
	if got := (stats.Estimate{Center: 10, Lo: math.Inf(-1), Hi: math.Inf(1)}).Spread(); !math.IsInf(got, 1) {
		t.Errorf("spread = %v", got)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
