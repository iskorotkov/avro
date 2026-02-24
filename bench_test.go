package avro_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

type Superhero struct {
	ID            int           `avro:"id"`
	AffiliationID int           `avro:"affiliation_id"`
	Name          string        `avro:"name"`
	Life          float32       `avro:"life"`
	Energy        float32       `avro:"energy"`
	Powers        []*Superpower `avro:"powers"`
}

type Superpower struct {
	ID      int     `avro:"id"`
	Name    string  `avro:"name"`
	Damage  float32 `avro:"damage"`
	Energy  float32 `avro:"energy"`
	Passive bool    `avro:"passive"`
}

type PartialSuperhero struct {
	ID            int     `avro:"id"`
	AffiliationID int     `avro:"affiliation_id"`
	Name          string  `avro:"name"`
	Life          float32 `avro:"life"`
	Energy        float32 `avro:"energy"`
}

func BenchmarkSuperheroDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{}

	b.ReportAllocs()

	for b.Loop() {
		_ = avro.Unmarshal(schema, data, super)
	}
}

func BenchmarkSuperheroEncode(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{
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

	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.Marshal(schema, super)
	}
}

func BenchmarkPartialSuperheroDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &PartialSuperhero{}

	b.ReportAllocs()

	for b.Loop() {
		_ = avro.Unmarshal(schema, data, super)
	}
}

func BenchmarkSuperheroGenericDecode(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		panic(err)
	}

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var m any
		_ = avro.Unmarshal(schema, data, &m)
	}
}

func BenchmarkSuperheroGenericEncode(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := map[string]any{
		"id":             234765,
		"affiliation_id": 9867,
		"Name":           "Wolverine",
		"life":           85.25,
		"energy":         32.75,
		"powers": []map[string]any{
			{"id": 2345, "name": "Bone Claws", "damage": 5, "energy": 1.15, "passive": false},
			{"id": 2346, "name": "Regeneration", "damage": -2, "energy": 0.55, "passive": true},
			{"id": 2347, "name": "Adamant skeleton", "damage": -10, "energy": 0, "passive": true},
		},
	}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.Marshal(schema, super)
	}
}

func BenchmarkSuperheroWriteFlush(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		panic(err)
	}

	super := &Superhero{
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

	b.ReportAllocs()

	w := avro.NewWriter(io.Discard, 128)
	for b.Loop() {
		w.WriteVal(schema, super)
		_ = w.Flush()
	}
}

func largeSuperhero(nPowers int) *Superhero {
	powers := make([]*Superpower, nPowers)
	for i := range nPowers {
		powers[i] = &Superpower{
			ID:      i,
			Name:    fmt.Sprintf("Power-%04d-of-the-universe", i),
			Damage:  float32(i) * 1.5,
			Energy:  float32(i) * 0.3,
			Passive: i%2 == 0,
		}
	}
	return &Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers:        powers,
	}
}

func BenchmarkDecodeSlabSize(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}

	data, err := avro.Marshal(schema, largeSuperhero(100))
	if err != nil {
		b.Fatal(err)
	}

	for _, size := range []int{128, 512, 1024, 4096} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			api := avro.Config{SlabSize: size}.Freeze()
			super := &Superhero{}

			b.ReportAllocs()

			for b.Loop() {
				_ = api.Unmarshal(schema, data, super)
			}
		})
	}
}

func BenchmarkEncodeWriteBufSize(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}

	super := largeSuperhero(100)

	for _, size := range []int{128, 512, 1024, 4096} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			api := avro.Config{WriteBufSize: size}.Freeze()

			b.ReportAllocs()

			enc := api.NewEncoder(schema, io.Discard)
			for b.Loop() {
				_ = enc.Encode(super)
			}
		})
	}
}

func BenchmarkDecodeReadBufSize(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}

	data, err := avro.Marshal(schema, largeSuperhero(100))
	if err != nil {
		b.Fatal(err)
	}

	for _, size := range []int{128, 512, 1024, 4096} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			api := avro.Config{ReadBufSize: size}.Freeze()
			super := &Superhero{}

			b.ReportAllocs()

			for b.Loop() {
				r := bytes.NewReader(data)
				dec := api.NewDecoder(schema, r)
				_ = dec.Decode(super)
			}
		})
	}
}
