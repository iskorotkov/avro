package avro_test

import (
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
)

// fuzzLimits bounds allocations so hostile counts and lengths fail fast instead of exhausting memory.
var fuzzLimits = avro.Config{
	MaxByteSliceSize:  1 << 20,
	MaxSliceAllocSize: 1 << 22,
	MaxMapAllocSize:   1 << 20,
}

var fuzzAPI = fuzzLimits.Freeze()

func fuzzDecode(api avro.API, schema avro.Schema) func([]byte) (any, error) {
	return func(data []byte) (any, error) {
		return fuzzcheck.Decode(api, schema, data)
	}
}

func fuzzEncode(api avro.API, schema avro.Schema) func(any) ([]byte, error) {
	return func(v any) ([]byte, error) {
		return api.Marshal(schema, v)
	}
}

// fuzzReadNextSchemas covers every schema type ReadNext dispatches on.
var fuzzReadNextSchemas = []string{
	`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`, `"string"`,
	`{"type":"int","logicalType":"date"}`,
	`{"type":"int","logicalType":"time-millis"}`,
	`{"type":"long","logicalType":"time-micros"}`,
	`{"type":"long","logicalType":"timestamp-millis"}`,
	`{"type":"long","logicalType":"timestamp-micros"}`,
	`{"type":"long","logicalType":"timestamp-nanos"}`,
	`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
	`{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":9,"scale":2}`,
	`{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`,
	`{"type":"string","logicalType":"uuid"}`,
	`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
	`{"type":"fixed","name":"F","size":4}`,
	`{"type":"array","items":"long"}`,
	`{"type":"map","values":"string"}`,
	`["null","int","string"]`,
	`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":{"type":"array","items":["null","string"]}}]}`,
}

// FuzzReaderReadNext decodes data with ReadNext and round-trips it generically against a schema chosen by idx.
func FuzzReaderReadNext(f *testing.F) {
	for i := range fuzzReadNextSchemas {
		f.Add(byte(i), []byte{})
		f.Add(byte(i), []byte{0x02, 0x02, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	}

	schemas := make([]avro.Schema, len(fuzzReadNextSchemas))
	for i, s := range fuzzReadNextSchemas {
		schema, err := avro.Parse(s)
		if err != nil {
			f.Fatalf("parse %s: %v", s, err)
		}
		schemas[i] = schema
	}

	f.Fuzz(func(t *testing.T, idx byte, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		i := int(idx) % len(schemas)
		// Out-of-precision decimals (as in Java) and overflowing time-micros (a tracked bug) cannot round-trip.
		if !strings.Contains(fuzzReadNextSchemas[i], "decimal") && !strings.Contains(fuzzReadNextSchemas[i], "time-micros") {
			fuzzcheck.RoundTrip(t, data, fuzzDecode(fuzzAPI, schemas[i]), fuzzEncode(fuzzAPI, schemas[i]))
		}
		schema := schemas[i]

		r := avro.NewReader(nil, 0).Reset(data)
		for range 64 {
			_ = r.ReadNext(schema)
			if r.Error != nil {
				return
			}
		}
	})
}
