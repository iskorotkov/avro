package run

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"golang.org/x/perf/benchfmt"
)

func TestBenchRegex(t *testing.T) {
	got := Options{Dataset: "superhero|nyctaxi", Op: "encode"}.BenchRegex()
	want := "^BenchmarkSuite$/^dataset=(superhero|nyctaxi)$/^op=(encode)$/^format=(.*)$"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTestArgs(t *testing.T) {
	o := Options{Dir: "/b", Procs: 4, Benchtime: "10ms", DataDir: "data", CPUProfileDir: "prof"}
	args := strings.Join(o.testArgs(2), " ")
	for _, want := range []string{"-test.cpu=4", "-test.benchtime=10ms", "-suite.rotate=2", "-suite.data=/b/data", "-test.cpuprofile=/b/prof/round-2.pprof"} {
		if !strings.Contains(args, want) {
			t.Errorf("args %q lack %q", args, want)
		}
	}
}

func TestHeaderParses(t *testing.T) {
	var buf bytes.Buffer
	h := Header{Commit: "abc-dirty", Date: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Go: "go1.26.0", GOMAXPROCS: 4, Flags: "-count=6 -dataset=.*"}
	if _, err := h.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("BenchmarkSuite/dataset=d/op=encode/format=avro-4 1 100 ns/op\n")

	r := benchfmt.NewReader(&buf, "header")
	if !r.Scan() {
		t.Fatal(r.Err())
	}
	res, ok := r.Result().(*benchfmt.Result)
	if !ok {
		t.Fatalf("unexpected record %T", r.Result())
	}
	for key, want := range map[string]string{"commit": "abc-dirty", "go": "go1.26.0", "gomaxprocs": "4", "flags": "-count=6 -dataset=.*", "date": "2026-01-02T03:04:05Z"} {
		if got := res.GetConfig(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}
