package superhero_test

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
)

const sampleSize = 1000

var update = flag.Bool("update", false, "regenerate sample.jsonl.zst")

func TestSample(t *testing.T) {
	want := superhero.Generate(sampleSize)
	if *update {
		writeSample(t, want)
	}

	ds, err := superhero.Provider{}.Load("", 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dataset.Typed[superhero.Superhero](ds)
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

func writeSample(t *testing.T, records []superhero.Superhero) {
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
