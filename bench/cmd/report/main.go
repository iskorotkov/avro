// Command report runs the benchmark suite (or reads a raw results file) and writes RESULTS.md and SVG charts.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/iskorotkov/avro/bench/report/jmh"
	"github.com/iskorotkov/avro/bench/report/pipeline"
	"github.com/iskorotkov/avro/bench/report/raw"
	"github.com/iskorotkov/avro/bench/report/run"
	"golang.org/x/perf/benchfmt"
)

type config struct {
	run      run.Options
	from     string
	java     string
	out      string
	charts   string
	baseline string
	noise    float64
}

func main() {
	if err := runMain(); err != nil {
		fmt.Fprintln(os.Stderr, "report:", err)
		os.Exit(1)
	}
}

func runMain() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return execute(ctx, parseFlags(os.Args[1:]))
}

func parseFlags(args []string) config {
	var c config
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	fs.IntVar(&c.run.Count, "count", 6, "number of rounds, each a separate process with a rotated format order")
	fs.IntVar(&c.run.Procs, "procs", min(4, runtime.NumCPU()), "GOMAXPROCS for the benchmark processes")
	fs.StringVar(&c.run.Benchtime, "benchtime", "200ms", "value of -test.benchtime")
	fs.StringVar(&c.run.Dataset, "dataset", "", "regexp of datasets to run (default all)")
	fs.StringVar(&c.run.Op, "op", "", "regexp of ops to run (default all)")
	fs.StringVar(&c.run.Format, "format", "", "regexp of formats to run (default all)")
	fs.StringVar(&c.run.Tags, "tags", "", "comma-separated build tags for the suite binary")
	fs.StringVar(&c.run.GoExperiment, "goexperiment", "", "GOEXPERIMENT value for the suite binary, e.g. jsonv2")
	fs.StringVar(&c.run.DataDir, "data", "", "dataset directory passed to the suite as -suite.data")
	fs.StringVar(&c.run.CPUProfileDir, "cpuprofile", "", "directory for one CPU profile per round")
	fs.StringVar(&c.run.ResultsDir, "results", "results", "directory for raw result files")
	fs.StringVar(&c.from, "from", "", "read this raw results file instead of running the suite")
	fs.StringVar(&c.java, "java", "", "JMH JSON file to merge as extra formats")
	fs.StringVar(&c.out, "out", "RESULTS.md", "markdown report path")
	fs.StringVar(&c.charts, "charts", "results/charts", "SVG charts directory")
	fs.StringVar(&c.baseline, "baseline", "avro", "baseline format (100%)")
	fs.Float64Var(&c.noise, "noise", 0.05, "CI half-width over median above which a result is flagged high-variance")
	_ = fs.Parse(args)
	c.run.Dir = "."
	c.run.Package = "./suite"
	return c
}

func execute(ctx context.Context, c config) error {
	rawPath := c.from
	if rawPath == "" {
		var err error
		if rawPath, err = run.Execute(ctx, c.run, os.Stderr, time.Now()); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "raw results:", rawPath)
	}
	results, err := load(rawPath, c.java)
	if err != nil {
		return err
	}
	out, err := pipeline.Generate(results, pipeline.Options{
		Baseline:  c.baseline,
		Noise:     c.noise,
		ChartsRef: chartsRef(c.out, c.charts),
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.charts, 0o750); err != nil {
		return err
	}
	for _, f := range out.Charts {
		if err := os.WriteFile(filepath.Join(c.charts, f.Name), f.Data, 0o600); err != nil {
			return err
		}
	}
	if err := os.WriteFile(c.out, out.Markdown, 0o600); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s and %d charts in %s\n", c.out, len(out.Charts), c.charts)
	return nil
}

func load(rawPath, javaPath string) ([]*benchfmt.Result, error) {
	results, err := raw.ReadFile(rawPath)
	if err != nil {
		return nil, err
	}
	if javaPath == "" {
		return results, nil
	}
	f, err := os.Open(javaPath) //nolint:gosec // Reading the user-selected JMH file is the purpose.
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // Read-only file.
	java, err := jmh.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", javaPath, err)
	}
	return append(results, java...), nil
}

// chartsRef is the charts directory relative to the markdown file, with forward slashes.
func chartsRef(out, charts string) string {
	rel, err := filepath.Rel(filepath.Dir(out), charts)
	if err != nil {
		return filepath.ToSlash(charts)
	}
	return filepath.ToSlash(rel)
}
