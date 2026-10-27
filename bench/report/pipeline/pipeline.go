// Package pipeline turns raw benchmark results into the RESULTS.md text and its SVG charts.
package pipeline

import (
	"bytes"
	"path"

	"github.com/iskorotkov/avro/bench/report/chart"
	"github.com/iskorotkov/avro/bench/report/markdown"
	"github.com/iskorotkov/avro/bench/report/stats"
	"golang.org/x/perf/benchfmt"
)

// Options configures report generation.
type Options struct {
	Baseline string
	// Noise is the CI half-width over median above which a result is flagged.
	Noise float64
	// ChartsRef is the slash-separated charts directory as seen from the markdown file.
	ChartsRef string
}

// Output is everything the report writes to disk.
type Output struct {
	Markdown []byte
	Charts   []chart.File
}

// Generate summarises results, compares them with the baseline and renders markdown and charts.
func Generate(results []*benchfmt.Result, o Options) (Output, error) {
	data, err := stats.Summarize(results, o.Noise)
	if err != nil {
		return Output{}, err
	}
	cmp := stats.Compare(data, o.Baseline)

	var md bytes.Buffer
	err = markdown.Render(&md, cmp, markdown.Options{
		SummaryChart: path.Join(o.ChartsRef, chart.SummaryFileName),
		OpChart: func(dataset, op string) string {
			return path.Join(o.ChartsRef, chart.OpFileName(dataset, op))
		},
	})
	if err != nil {
		return Output{}, err
	}
	return Output{Markdown: md.Bytes(), Charts: chart.All(cmp)}, nil
}
