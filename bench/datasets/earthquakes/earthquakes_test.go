package earthquakes_test

import (
	"flag"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/earthquakes"
)

const sampleSize = 1707

var jsonPath = flag.String("update", "", "regenerate sample.jsonl.zst from this earthquakes.json file")

func TestSample(t *testing.T) {
	if *jsonPath != "" {
		writeSample(t, *jsonPath)
	}

	ds, err := earthquakes.Provider{}.Load("", 0)
	if err != nil {
		t.Fatal(err)
	}
	quakes, err := dataset.Typed[earthquakes.Quake](ds)
	if err != nil {
		t.Fatal(err)
	}
	if len(quakes) != sampleSize {
		t.Fatalf("sample has %d quakes, want %d", len(quakes), sampleSize)
	}
	if q := quakes[0]; q.ID == "" || q.Time.IsZero() || len(q.Geometry.Coordinates) != 3 || len(q.IDs) == 0 {
		t.Fatalf("first quake looks empty: %+v", q)
	}
}

func TestParseFeatures(t *testing.T) {
	const doc = `{"type":"FeatureCollection","features":[{"type":"Feature","id":"ci1",
"properties":{"mag":2,"place":"x","time":1517966773840,"updated":1517966996303,"tz":-480,"url":"u","detail":"d",
"felt":null,"cdi":3.5,"mmi":null,"alert":null,"status":"automatic","tsunami":0,"sig":62,"net":"ci","code":"1",
"ids":",ci1,us2,","sources":",ci,us,","types":",origin,","nst":7,"dmin":null,"rms":1,"gap":174,"magType":"ml","type":"earthquake","title":"t"},
"geometry":{"type":"Point","coordinates":[-118.6,34.2,13]}}]}`

	got, err := earthquakes.ParseFeatures([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d quakes, want 1", len(got))
	}

	q := got[0]
	if q.Time.UnixMilli() != 1517966773840 || q.Time.Location().String() != "UTC" {
		t.Errorf("time = %v", q.Time)
	}
	if diff := cmp.Diff([]string{"ci1", "us2"}, q.IDs); diff != "" {
		t.Errorf("ids: %s", diff)
	}
	if q.Felt != nil || q.CDI == nil || *q.CDI != 3.5 || q.Mag != 2 {
		t.Errorf("nullable or numeric fields: %+v", q)
	}
	if diff := cmp.Diff([]float64{-118.6, 34.2, 13}, q.Geometry.Coordinates); diff != "" {
		t.Errorf("coordinates: %s", diff)
	}
}

func TestParseFeaturesMalformedList(t *testing.T) {
	const doc = `{"features":[{"id":"a","properties":{"ids":"a","sources":",ci,","types":",x,"}}]}`
	if _, err := earthquakes.ParseFeatures([]byte(doc)); err == nil {
		t.Fatal("want error for list without commas")
	}
}

func writeSample(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	quakes, err := earthquakes.ParseFeatures(data)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Create("sample.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := dataset.WriteJSONL(f, quakes); err != nil {
		t.Fatal(err)
	}
}
