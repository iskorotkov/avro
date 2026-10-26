package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

const unionBenchSchema = `{"type":"record","name":"UnionBench","namespace":"bench","fields":[
	{"name":"u","type":["null","string",{"type":"record","name":"P","fields":[
		{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":{"type":"array","items":"long"}}]}]},
	{"name":"z","type":"string"}]}`

type UnionBenchP struct {
	A int     `avro:"a"`
	B string  `avro:"b"`
	C []int64 `avro:"c"`
}

func unionBenchValue() map[string]any {
	return map[string]any{
		"u": map[string]any{"bench.P": map[string]any{"a": 1, "b": "hello", "c": []int64{1, 2, 3}}},
		"z": "ok",
	}
}

func unionBenchData(b *testing.B, schema avro.Schema) []byte {
	b.Helper()

	data, err := avro.Marshal(schema, unionBenchValue())
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func BenchmarkUnionGenericDecode(b *testing.B) {
	schema := avro.MustParse(unionBenchSchema)
	data := unionBenchData(b, schema)

	b.ReportAllocs()

	for b.Loop() {
		var got any
		_ = avro.Unmarshal(schema, data, &got)
	}
}

func BenchmarkUnionGenericEncode(b *testing.B) {
	schema := avro.MustParse(unionBenchSchema)
	value := unionBenchValue()

	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.Marshal(schema, value)
	}
}

func BenchmarkUnionMapStructDecode(b *testing.B) {
	schema := avro.MustParse(unionBenchSchema)
	data := unionBenchData(b, schema)

	b.ReportAllocs()

	for b.Loop() {
		var got struct {
			U map[string]any `avro:"u"`
			Z string         `avro:"z"`
		}
		_ = avro.Unmarshal(schema, data, &got)
	}
}

func BenchmarkUnionResolvedStructDecode(b *testing.B) {
	schema := avro.MustParse(unionBenchSchema)
	data := unionBenchData(b, schema)

	for _, bc := range []struct {
		name     string
		register bool
	}{
		{name: "registered", register: true},
		{name: "unregistered"},
	} {
		api := avro.Config{}.Freeze()
		if bc.register {
			api.Register("bench.P", UnionBenchP{})
		}

		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				var got struct {
					U any    `avro:"u"`
					Z string `avro:"z"`
				}
				_ = api.Unmarshal(schema, data, &got)
			}
		})
	}
}

func BenchmarkCodecBuildDisableCaching(b *testing.B) {
	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	if err != nil {
		b.Fatal(err)
	}

	api := avro.Config{DisableCaching: true}.Freeze()
	super := largeSuperhero(1)

	b.ReportAllocs()

	for b.Loop() {
		_, _ = api.Marshal(schema, super)
	}
}

func BenchmarkSchemaParse(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_, _ = avro.ParseWithCache(unionBenchSchema, "", &avro.SchemaCache{})
	}
}
