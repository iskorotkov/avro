package chart

import (
	"github.com/iskorotkov/avro/bench/report/stats"
)

// SummaryFileName is the file name of the summary chart.
const SummaryFileName = "summary.svg"

// File is a rendered chart and its file name.
type File struct {
	Name string
	Data []byte
}

// OpFileName is the file name of a dataset/op chart.
func OpFileName(dataset, op string) string { return dataset + "-" + op + ".svg" }

// All renders the summary and one chart per dataset/op that has relative times.
func All(c stats.Comparison) []File {
	files := []File{{Name: SummaryFileName, Data: Summary(c.Summary(), c.Baseline)}}
	for _, d := range c.Datasets {
		for _, op := range d.Ops {
			if op.HasRelativeTime() {
				files = append(files, File{Name: OpFileName(d.Name, op.Name), Data: Op(d.Name, op, c.Baseline)})
			}
		}
	}
	return files
}
