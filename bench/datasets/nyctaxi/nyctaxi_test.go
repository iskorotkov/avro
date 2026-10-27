package nyctaxi_test

import (
	"flag"
	"os"
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
)

// sampleStride keeps about 20 000 of the 2 964 624 trips in the January 2024 file.
const sampleStride = 148

var parquetPath = flag.String("update", "", "regenerate sample.jsonl.zst from this parquet file")

func TestSample(t *testing.T) {
	if *parquetPath != "" {
		writeSample(t, *parquetPath)
	}

	ds, err := nyctaxi.Provider{}.Load("")
	if err != nil {
		t.Fatal(err)
	}
	trips, err := dataset.Typed[nyctaxi.Trip](ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(trips) < 10000 {
		t.Fatalf("sample has %d trips, want at least 10000", len(trips))
	}
	if trips[0].PickupTime.IsZero() || trips[0].TotalAmount == 0 {
		t.Fatalf("first trip looks empty: %+v", trips[0])
	}
}

func writeSample(t *testing.T, path string) {
	t.Helper()

	trips, err := nyctaxi.ReadParquet(path, sampleStride)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Create("sample.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := dataset.WriteJSONL(f, trips); err != nil {
		t.Fatal(err)
	}
}
