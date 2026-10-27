package suite_test

import (
	"flag"
	"regexp"
	"slices"
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats"
	"github.com/iskorotkov/avro/bench/suite"
)

var (
	rotate  = flag.Int("suite.rotate", 0, "rotate the format order by `n` to interleave formats across rounds")
	dataDir = flag.String("suite.data", "", "load full datasets from `dir` instead of the committed samples")
	rows    = flag.Int("suite.rows", 0, "load at most `n` records per dataset (0 loads all)")
	dsMatch = flag.String("suite.dataset", "", "load only datasets matching `regexp`")
	fmMatch = flag.String("suite.format", "", "bind only formats matching `regexp`")
)

func TestRoundTrip(t *testing.T) {
	for _, c := range load(t) {
		t.Run(c.Dataset+"/"+c.Format, func(t *testing.T) {
			if err := format.Verify(c.Subject); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkSuite(b *testing.B) {
	cases := load(b)
	for _, c := range cases {
		if err := format.Verify(c.Subject); err != nil {
			b.Fatalf("%s/%s: %v", c.Dataset, c.Format, err)
		}
	}

	for _, ds := range datasetNames(cases) {
		b.Run("dataset="+ds, func(b *testing.B) {
			benchDataset(b, filter(cases, ds))
		})
	}
}

func benchDataset(b *testing.B, cases []suite.Case) {
	for _, op := range opNames(cases) {
		b.Run("op="+op, func(b *testing.B) {
			for _, c := range cases {
				if run := find(c.Subject, op); run != nil {
					b.Run("format="+c.Format, func(b *testing.B) { run(b, c.Subject) })
				}
			}
		})
	}
}

func load(tb testing.TB) []suite.Case {
	tb.Helper()

	providers := matching(tb, *dsMatch, datasets.All(), dataset.Provider.Name)
	fs := matching(tb, *fmMatch, formats.All(), format.Format.Name)

	cases, err := suite.Load(*dataDir, *rows, providers, suite.Rotate(fs, *rotate))
	if err != nil {
		tb.Fatal(err)
	}

	return cases
}

func matching[T any](tb testing.TB, pattern string, items []T, name func(T) string) []T {
	tb.Helper()

	re, err := regexp.Compile(pattern)
	if err != nil {
		tb.Fatal(err)
	}

	return slices.DeleteFunc(items, func(item T) bool { return !re.MatchString(name(item)) })
}

func datasetNames(cases []suite.Case) []string {
	var names []string
	for _, c := range cases {
		if !slices.Contains(names, c.Dataset) {
			names = append(names, c.Dataset)
		}
	}

	return names
}

func filter(cases []suite.Case, ds string) []suite.Case {
	return slices.DeleteFunc(slices.Clone(cases), func(c suite.Case) bool { return c.Dataset != ds })
}

func opNames(cases []suite.Case) []string {
	var names []string
	for _, c := range cases {
		for _, op := range suite.Ops(c.Subject) {
			if !slices.Contains(names, op.Name) {
				names = append(names, op.Name)
			}
		}
	}
	slices.SortFunc(names, suite.CompareOps)

	return names
}

func find(s *format.Subject, name string) func(*testing.B, *format.Subject) {
	for _, op := range suite.Ops(s) {
		if op.Name == name {
			return op.Run
		}
	}

	return nil
}
