// Package jmh converts JMH JSON output into benchfmt results so Java numbers join the Go ones.
//
// # Input
//
// Native JMH JSON, produced by
//
//	java -jar benchmarks.jar -rf json -rff out.json -prof gc
//
// in average-time mode (mode "avgt"); any other mode is rejected.
// The input is a top-level array with one object per benchmark and parameter combination.
//
// # Mapping
//
// Each benchmark object becomes one series named
// BenchmarkSuite/dataset=<d>/op=<op>/format=<f>, where
//
//   - params.dataset (required) and params.op (required) use the same vocabulary as the Go suite
//     (encode, decode, batch-encode, batch-decode, container-write-<codec>, container-read-<codec>);
//   - params.format (optional) defaults to "java-avro".
//
// One JMH op must process the whole dataset sample, like one Go benchmark iteration.
// Every value in primaryMetric.rawData (all forks flattened) is one sample.
// primaryMetric.scoreUnit is ns/op, us/op, ms/op or s/op and is converted to sec/op.
//
// # Sizes
//
// The optional params records (records per op) and wireBytes (encoded bytes per op) are
// constants the benchmark knows in @Setup; records adds sec/rec and rec/s, wireBytes adds B/s
// and, with records, wire-B/rec.
//
// # Secondary metrics
//
// Sample i of a secondary metric is its rawData[i] when rawData has one value per primary sample,
// otherwise its score. Only gc.alloc.rate.norm (legacy "·gc.alloc.rate.norm" also accepted) is read,
// as bytes allocated per op, and becomes B/op.
//
// Other secondary metrics are ignored.
package jmh

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/perf/benchfmt"
)

// DefaultFormat names results whose params carry no format.
const DefaultFormat = "java-avro"

type metric struct {
	Score     float64     `json:"score"`
	ScoreUnit string      `json:"scoreUnit"`
	RawData   [][]float64 `json:"rawData"`
}

type benchmark struct {
	Benchmark        string            `json:"benchmark"`
	Mode             string            `json:"mode"`
	Params           map[string]string `json:"params"`
	PrimaryMetric    metric            `json:"primaryMetric"`
	SecondaryMetrics map[string]metric `json:"secondaryMetrics"`
}

// Parse reads JMH JSON and returns one benchfmt result per measured iteration.
func Parse(r io.Reader) ([]*benchfmt.Result, error) {
	var benchmarks []benchmark
	if err := json.NewDecoder(r).Decode(&benchmarks); err != nil {
		return nil, fmt.Errorf("decode JMH json: %w", err)
	}
	var out []*benchfmt.Result
	for _, b := range benchmarks {
		results, err := b.results()
		if err != nil {
			return nil, fmt.Errorf("benchmark %q: %w", b.Benchmark, err)
		}
		out = append(out, results...)
	}
	return out, nil
}

func (b benchmark) results() ([]*benchfmt.Result, error) {
	if b.Mode != "avgt" {
		return nil, fmt.Errorf("unsupported mode %q, only avgt is accepted", b.Mode)
	}
	dataset, op := b.Params["dataset"], b.Params["op"]
	if dataset == "" || op == "" {
		return nil, fmt.Errorf("params.dataset and params.op are required")
	}
	format := b.Params["format"]
	if format == "" {
		format = DefaultFormat
	}
	factor, err := secondsPerUnit(b.PrimaryMetric.ScoreUnit)
	if err != nil {
		return nil, err
	}

	raw := slices.Concat(b.PrimaryMetric.RawData...)
	if len(raw) == 0 {
		raw = []float64{b.PrimaryMetric.Score}
	}
	name := fmt.Sprintf("BenchmarkSuite/dataset=%s/op=%s/format=%s", dataset, op, format)

	out := make([]*benchfmt.Result, len(raw))
	for i, v := range raw {
		sec := v * factor
		values := []benchfmt.Value{{Value: sec, Unit: "sec/op"}}
		values = append(values, b.secondaryValues(i, len(raw), sec)...)
		out[i] = &benchfmt.Result{Name: benchfmt.Name(name), Iters: 1, Values: values}
	}
	return out, nil
}

func (b benchmark) secondaryValues(i, n int, sec float64) []benchfmt.Value {
	var values []benchfmt.Value
	if alloc, ok := b.secondary("gc.alloc.rate.norm", i, n); ok {
		values = append(values, benchfmt.Value{Value: alloc, Unit: "B/op"})
	} else if alloc, ok := b.secondary("·gc.alloc.rate.norm", i, n); ok {
		values = append(values, benchfmt.Value{Value: alloc, Unit: "B/op"})
	}
	records, hasRecords := b.param("records")
	hasRecords = hasRecords && records > 0
	if hasRecords {
		values = append(values,
			benchfmt.Value{Value: sec / records, Unit: "sec/rec"},
			benchfmt.Value{Value: records / sec, Unit: "rec/s"})
	}
	if wire, ok := b.param("wireBytes"); ok {
		values = append(values, benchfmt.Value{Value: wire / sec, Unit: "B/s"})
		if hasRecords {
			values = append(values, benchfmt.Value{Value: wire / records, Unit: "wire-B/rec"})
		}
	}
	return values
}

func (b benchmark) secondary(name string, i, n int) (float64, bool) {
	m, ok := b.SecondaryMetrics[name]
	if !ok {
		return 0, false
	}
	if raw := slices.Concat(m.RawData...); len(raw) == n {
		return raw[i], true
	}
	return m.Score, true
}

func (b benchmark) param(name string) (float64, bool) {
	v, err := strconv.ParseFloat(b.Params[name], 64)
	return v, err == nil
}

func secondsPerUnit(unit string) (float64, error) {
	switch strings.TrimSpace(unit) {
	case "ns/op":
		return 1e-9, nil
	case "us/op", "µs/op":
		return 1e-6, nil
	case "ms/op":
		return 1e-3, nil
	case "s/op":
		return 1, nil
	}
	return 0, fmt.Errorf("unsupported scoreUnit %q", unit)
}
