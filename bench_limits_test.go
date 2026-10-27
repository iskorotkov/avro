package avro_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func BenchmarkSuperheroReadNext(b *testing.B) {
	data, err := os.ReadFile("testdata/superhero.bin")
	if err != nil {
		b.Fatal(err)
	}
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}
	r := avro.NewReader(nil, 0)

	b.ReportAllocs()

	for b.Loop() {
		_ = r.Reset(data).ReadNext(schema)
	}
}

type benchListNode struct {
	V    int64          `avro:"v"`
	Next *benchListNode `avro:"next"`
}

func BenchmarkRecursiveRecordDecode(b *testing.B) {
	schema := avro.MustParse(`{"type":"record","name":"N","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","N"]}]}`)
	// A 100-node list: each node is a value followed by the union branch of its successor.
	data := append(bytes.Repeat([]byte{0x02, 0x02}, 99), 0x02, 0x00)
	if err := avro.Unmarshal(schema, data, &benchListNode{}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		var got benchListNode
		_ = avro.Unmarshal(schema, data, &got)
	}
}
