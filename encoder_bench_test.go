package avro_test

import (
	"io"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func BenchmarkNewEncoderForSchema(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = avro.NewEncoderForSchema(schema, io.Discard)
	}
}

func BenchmarkEncoder_Reset(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}
	enc := avro.NewEncoderForSchema(schema, io.Discard)
	super := &Superhero{ID: 1, Name: "Hero", Powers: []*Superpower{{ID: 1, Name: "flight"}}}

	b.ReportAllocs()
	for b.Loop() {
		enc.Reset(io.Discard)
		_ = enc.Encode(super)
	}
}
