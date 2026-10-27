package dataset_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/bench/dataset"
)

type rec struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJSONLRoundTrip(t *testing.T) {
	in := []rec{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}

	var buf bytes.Buffer
	if err := dataset.WriteJSONL(&buf, in); err != nil {
		t.Fatal(err)
	}

	out, err := dataset.DecodeJSONL[rec](buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("got %v, want %v", out, in)
	}
}

func TestTyped(t *testing.T) {
	ds := dataset.New("test", "{}", []rec{{ID: 1}})

	got, err := dataset.Typed[rec](ds)
	if err != nil || got[0].ID != 1 {
		t.Fatalf("Typed() = %v, %v", got, err)
	}
	if _, err := dataset.Typed[int](ds); err == nil {
		t.Fatal("Typed[int]() expected error")
	}
	if _, ok := ds.New().(*rec); !ok {
		t.Fatalf("New() = %T", ds.New())
	}
}
