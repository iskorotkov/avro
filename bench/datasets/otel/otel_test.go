package otel_test

import (
	"flag"
	"os"
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/otel"
)

const sampleSize = 500

var update = flag.Bool("update", false, "regenerate sample.jsonl.zst")

func TestSample(t *testing.T) {
	want := otel.Generate(sampleSize)
	if *update {
		writeSample(t, want)
	}

	ds, err := otel.Provider{}.Load("", 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dataset.Typed[otel.ResourceSpans](ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("sample has %d records, want %d; run go test -update", len(got), len(want))
	}
	for i := range want {
		if !want[i].Equal(*got[i]) {
			t.Fatalf("record %d is stale, run go test -update", i)
		}
	}
}

func TestLoadRows(t *testing.T) {
	ds, err := otel.Provider{}.Load("", 7)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ds.Records()); n != 7 {
		t.Fatalf("got %d records, want 7", n)
	}
}

func writeSample(t *testing.T, records []otel.ResourceSpans) {
	t.Helper()

	f, err := os.Create("sample.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := dataset.WriteJSONL(f, records); err != nil {
		t.Fatal(err)
	}
}
