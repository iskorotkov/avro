package avro_test

import (
	"os"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func newBenchSuperhero() *Superhero {
	return &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}
}

func BenchmarkSuperheroAppendMarshal(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}
	super := newBenchSuperhero()
	buf := make([]byte, 0, 256)

	b.ReportAllocs()

	for b.Loop() {
		buf, _ = avro.AppendMarshal(buf[:0], schema, super)
	}
}

func BenchmarkSuperheroDecodeAliasInput(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}
	api := avro.Config{AliasInput: true}.Freeze()

	b.ReportAllocs()

	for b.Loop() {
		super := &Superhero{}
		_ = api.Unmarshal(schema, data, super)
	}
}

func BenchmarkSuperheroDecodeCopyInput(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}
	api := avro.Config{}.Freeze()

	b.ReportAllocs()

	for b.Loop() {
		super := &Superhero{}
		_ = api.Unmarshal(schema, data, super)
	}
}
