package wikirc_test

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/wikirc"
)

const sampleSize = 2500

var update = flag.Bool("update", false, "regenerate sample.jsonl.zst")

func TestSample(t *testing.T) {
	want := wikirc.Generate(sampleSize)
	if *update {
		writeSample(t, want)
	}

	ds, err := wikirc.Provider{}.Load("", 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dataset.Typed[wikirc.RecentChange](ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("sample has %d records, want %d; run go test -update", len(got), len(want))
	}
	for i := range want {
		if diff := cmp.Diff(want[i], *got[i]); diff != "" {
			t.Fatalf("record %d is stale, run go test -update: %s", i, diff)
		}
	}
}

func TestLoadRows(t *testing.T) {
	ds, err := wikirc.Provider{}.Load("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(ds.Records()); got != 10 {
		t.Fatalf("Load(rows=10) returned %d records", got)
	}
}

func writeSample(t *testing.T, records []wikirc.RecentChange) {
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
