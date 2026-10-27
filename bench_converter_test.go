package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func passThroughConverter(typ avro.Type) avro.TypeConversionFuncs {
	pass := func(in any, _ avro.Schema) (any, error) { return in, nil }
	return avro.TypeConversionFuncs{AvroType: typ, EncoderTypeConversion: pass, DecoderTypeConversion: pass}
}

func BenchmarkUnionMapEncode(b *testing.B) {
	schema := avro.MustParse(`["null","string","long"]`)

	for _, bc := range []struct {
		name  string
		value map[string]any
	}{
		{name: "long", value: map[string]any{"long": int64(42)}},
		{name: "string", value: map[string]any{"string": "hello"}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := avro.Marshal(schema, bc.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTypeConvertersDecode(b *testing.B) {
	convs := avro.NewTypeConverters()
	convs.RegisterTypeConverters(passThroughConverter(avro.Long))

	for _, bc := range []struct {
		name   string
		schema avro.Schema
	}{
		{name: "hit", schema: avro.NewPrimitiveSchema(avro.Long, nil)},
		{name: "miss", schema: avro.NewPrimitiveSchema(avro.Int, nil)},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				_, _ = convs.DecodeTypeConvert(int64(42), bc.schema)
			}
		})
	}
}

func BenchmarkGenericDecodeWithConverter(b *testing.B) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)
	data, err := avro.Marshal(schema, map[string]any{"a": int64(42), "b": "hello"})
	if err != nil {
		b.Fatal(err)
	}

	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(passThroughConverter(avro.Long))

	b.ReportAllocs()

	for b.Loop() {
		var got map[string]any
		if err := api.Unmarshal(schema, data, &got); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNullableUnionConverterDecode(b *testing.B) {
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(passThroughConverter(avro.Union))

	b.Run("ptr", func(b *testing.B) {
		schema := avro.MustParse(`["null","long"]`)
		data, err := avro.Marshal(schema, int64(42))
		if err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()

		for b.Loop() {
			var got *int64
			if err := api.Unmarshal(schema, data, &got); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("slice", func(b *testing.B) {
		schema := avro.MustParse(`["null","bytes"]`)
		data, err := avro.Marshal(schema, []byte("hello"))
		if err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()

		for b.Loop() {
			var got []byte
			if err := api.Unmarshal(schema, data, &got); err != nil {
				b.Fatal(err)
			}
		}
	})
}
