package stats

import (
	"fmt"
	"slices"

	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchmath"
	"golang.org/x/perf/benchproc"
)

// Confidence is the confidence level of every reported interval.
const Confidence = 0.95

// Summarize groups results by dataset, op and format and summarises each metric as a median with CI.
func Summarize(results []*benchfmt.Result, noise float64) (Data, error) {
	var parser benchproc.ProjectionParser
	proj, err := parser.Parse("/dataset,/op,/format", nil)
	if err != nil {
		return Data{}, fmt.Errorf("parse projection: %w", err)
	}
	fields := proj.Fields()

	samples := make(map[Key]map[Metric][]float64)
	for _, res := range results {
		pk := proj.Project(res)
		key := Key{Dataset: pk.Get(fields[0]), Op: pk.Get(fields[1]), Format: pk.Get(fields[2])}
		if key.Dataset == "" || key.Op == "" || key.Format == "" {
			continue
		}
		if samples[key] == nil {
			samples[key] = make(map[Metric][]float64)
		}
		for _, m := range AllMetrics() {
			if v, ok := res.Value(string(m)); ok {
				samples[key][m] = append(samples[key][m], v)
			}
		}
	}

	data := Data{Env: readEnv(results), Estimates: make(map[Key]map[Metric]Estimate, len(samples))}
	for key, byMetric := range samples {
		data.Estimates[key] = make(map[Metric]Estimate, len(byMetric))
		for m, values := range byMetric {
			data.Estimates[key][m] = estimate(values, noise)
		}
	}
	return data, nil
}

func estimate(values []float64, noise float64) Estimate {
	sample := benchmath.NewSample(values, &benchmath.DefaultThresholds)
	s := benchmath.AssumeNothing.Summary(sample, Confidence)
	e := Estimate{Center: s.Center, Lo: s.Lo, Hi: s.Hi}
	if slices.Min(values) == slices.Max(values) {
		return Estimate{Center: values[0], Lo: values[0], Hi: values[0]}
	}
	e.Noisy = len(s.Warnings) > 0 || e.Spread() > noise
	return e
}

func readEnv(results []*benchfmt.Result) Env {
	var env Env
	targets := []struct {
		key string
		dst *string
	}{
		{"cpu", &env.CPU},
		{"goos", &env.GOOS},
		{"goarch", &env.GOARCH},
		{"go", &env.Go},
		{"commit", &env.Commit},
		{"date", &env.Date},
		{"gomaxprocs", &env.GOMAXPROCS},
		{"flags", &env.Flags},
	}
	for _, res := range results {
		for _, t := range targets {
			if *t.dst == "" {
				*t.dst = res.GetConfig(t.key)
			}
		}
	}
	return env
}
