package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

type embedInner struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type embeddedValRecord struct {
	C string `avro:"c"`
	embedInner
}

type embeddedPtrRecord struct {
	C string `avro:"c"`
	*embedInner
}

const embeddedRecordSchema = `{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"string"}]}`

var embeddedRecordData = []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}

func BenchmarkEmbeddedValDecode(b *testing.B) {
	schema := avro.MustParse(embeddedRecordSchema)
	got := &embeddedValRecord{}

	b.ReportAllocs()

	for b.Loop() {
		_ = avro.Unmarshal(schema, embeddedRecordData, got)
	}
}

func BenchmarkEmbeddedPtrDecode(b *testing.B) {
	schema := avro.MustParse(embeddedRecordSchema)
	got := &embeddedPtrRecord{embedInner: &embedInner{}}

	b.ReportAllocs()

	for b.Loop() {
		_ = avro.Unmarshal(schema, embeddedRecordData, got)
	}
}

func BenchmarkEmbeddedValEncode(b *testing.B) {
	schema := avro.MustParse(embeddedRecordSchema)
	rec := &embeddedValRecord{C: "bar", embedInner: embedInner{A: 27, B: "foo"}}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.Marshal(schema, rec)
	}
}

func BenchmarkEmbeddedPtrEncode(b *testing.B) {
	schema := avro.MustParse(embeddedRecordSchema)
	rec := &embeddedPtrRecord{C: "bar", embedInner: &embedInner{A: 27, B: "foo"}}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.Marshal(schema, rec)
	}
}
