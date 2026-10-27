package jmh_test

import (
	"math"
	"os"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/bench/report/jmh"
)

func TestParseExample(t *testing.T) {
	f, err := os.Open("testdata/example.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	results, err := jmh.Parse(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 6 {
		t.Fatalf("got %d results, want 6", len(results))
	}

	first := results[0]
	if got, want := first.Name.String(), "BenchmarkSuite/dataset=superhero/op=encode/format=java-avro"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	want := map[string]float64{
		"sec/op": 1.9e-3, "B/op": 4096, "sec/rec": 1.9e-6, "rec/s": 1000 / 1.9e-3, "B/s": 50000 / 1.9e-3, "wire-B/rec": 50,
	}
	for unit, w := range want {
		got, ok := first.Value(unit)
		if !ok || math.Abs(got-w) > 1e-9*math.Abs(w) {
			t.Errorf("%s = %v (ok=%v), want %v", unit, got, ok, w)
		}
	}

	last := results[5]
	if got := last.Name.String(); !strings.HasSuffix(got, "op=decode/format=java-avro") {
		t.Errorf("default format missing in %q", got)
	}
	if got, _ := last.Value("sec/op"); math.Abs(got-510e-6) > 1e-12 {
		t.Errorf("sec/op = %v, want 510us", got)
	}
}

func TestParseRejects(t *testing.T) {
	tests := map[string]string{
		"mode":    `[{"benchmark":"b","mode":"thrpt","params":{"dataset":"d","op":"encode"},"primaryMetric":{"scoreUnit":"ns/op"}}]`,
		"params":  `[{"benchmark":"b","mode":"avgt","params":{"op":"encode"},"primaryMetric":{"scoreUnit":"ns/op"}}]`,
		"unit":    `[{"benchmark":"b","mode":"avgt","params":{"dataset":"d","op":"encode"},"primaryMetric":{"scoreUnit":"ops/s"}}]`,
		"invalid": `{`,
	}
	for name, input := range tests {
		if _, err := jmh.Parse(strings.NewReader(input)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
