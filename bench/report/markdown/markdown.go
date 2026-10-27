// Package markdown renders a stats.Comparison as the RESULTS.md document.
package markdown

import (
	"fmt"
	"io"
	"strings"

	"github.com/iskorotkov/avro/bench/report/stats"
)

// Options controls the links embedded in the document.
type Options struct {
	// SummaryChart is the image path of the summary chart.
	SummaryChart string
	// OpChart returns the image path of a dataset/op chart.
	OpChart func(dataset, op string) string
}

// Render writes the full report.
func Render(w io.Writer, c stats.Comparison, o Options) error {
	var b strings.Builder
	b.WriteString("# Benchmark results\n\n")
	writeEnv(&b, c.Env)
	fmt.Fprintf(&b, "How to read: percentages are relative to the `%s` baseline (100%%, lower is better); "+
		"± is the 95%% CI half-width; ⚠ marks high variance or too few samples.\n\n", c.Baseline)
	if o.SummaryChart != "" {
		fmt.Fprintf(&b, "![Summary](%s)\n\n", o.SummaryChart)
	}
	for _, d := range c.Datasets {
		fmt.Fprintf(&b, "## %s\n\n", d.Name)
		for _, op := range d.Ops {
			writeOp(&b, d.Name, op, o)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeEnv(b *strings.Builder, e stats.Env) {
	rows := [][2]string{
		{"CPU", e.CPU},
		{"OS/arch", joinNonEmpty("/", e.GOOS, e.GOARCH)},
		{"Go", e.Go},
		{"Commit", e.Commit},
		{"Date", e.Date},
		{"GOMAXPROCS", e.GOMAXPROCS},
		{"Flags", e.Flags},
	}
	b.WriteString("| | |\n|---|---|\n")
	for _, r := range rows {
		val := cell(r[1])
		if r[0] == "Flags" && r[1] != "" {
			val = "`" + val + "`"
		}
		fmt.Fprintf(b, "| %s | %s |\n", r[0], val)
	}
	b.WriteString("\n")
}

func writeOp(b *strings.Builder, dataset string, op stats.Op, o Options) {
	fmt.Fprintf(b, "### %s\n\n", op.Name)
	if o.OpChart != nil && op.HasRelativeTime() {
		fmt.Fprintf(b, "![%s %s](%s)\n\n", dataset, op.Name, o.OpChart(dataset, op.Name))
	}
	b.WriteString("| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range op.Rows {
		fmt.Fprintf(b, "| %s |", formatName(r))
		for _, c := range rowCells(r) {
			fmt.Fprintf(b, " %s |", c)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")
}

func rowCells(r stats.Row) []string {
	return []string{
		absolute(r, stats.SecPerOp, seconds),
		relative(r, stats.SecPerRec),
		absolute(r, stats.RecPerSec, number),
		absolute(r, stats.BytesPerSec, byteRate),
		absolute(r, stats.BytesPerOp, bytes),
		relative(r, stats.BytesPerOp),
		absolute(r, stats.AllocsPerOp, number),
		relative(r, stats.AllocsPerOp),
		absolute(r, stats.WirePerRec, bytes),
		relative(r, stats.WirePerRec),
	}
}

func formatName(r stats.Row) string {
	if r.Baseline {
		return "**" + cell(r.Format) + "**"
	}
	return cell(r.Format)
}

func cell(s string) string {
	if s == "" {
		return missing
	}
	return strings.ReplaceAll(s, "|", `\|`)
}

func joinNonEmpty(sep string, parts ...string) string {
	var kept []string
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, sep)
}
