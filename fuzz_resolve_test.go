package avro_test

import (
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
)

// FuzzSchemaResolve resolves fuzzed reader/writer schemas and checks resolved data round-trips through the reader schema.
func FuzzSchemaResolve(f *testing.F) {
	type seed struct{ reader, writer, data string }
	seeds := []seed{
		{`"long"`, `"int"`, "\x02"},
		{`"double"`, `"float"`, "\x00\x00\xc0\x3f"},
		{`"bytes"`, `"string"`, "\x06foo"},
		{`["null","long"]`, `"int"`, "\x02"},
		{`"long"`, `["null","int"]`, "\x02\x02"},
		{
			`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			"\x04",
		},
		{
			`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"c","type":"string","default":"x"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			"\x02\x06foo",
		},
		{`{"type":"array","items":"long"}`, `{"type":"array","items":"int"}`, "\x04\x02\x04\x00"},
		{`{"type":"map","values":"double"}`, `{"type":"map","values":"long"}`, "\x02\x02a\x02\x00"},
	}
	for _, s := range seeds {
		f.Add(s.reader, s.writer, []byte(s.data))
	}

	f.Fuzz(func(t *testing.T, readerSchema, writerSchema string, data []byte) {
		if len(readerSchema)+len(writerSchema) > 1<<12 || len(data) > 1<<16 {
			return
		}
		reader, err := avro.ParseWithCache(readerSchema, "", &avro.SchemaCache{})
		if err != nil {
			return
		}
		writer, err := avro.ParseWithCache(writerSchema, "", &avro.SchemaCache{})
		if err != nil {
			return
		}
		resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
		if err != nil {
			return
		}
		// Decimals beyond their declared precision decode but do not encode, as in Java.
		if strings.Contains(strings.ToLower(readerSchema), "decimal") {
			var out any
			_ = fuzzAPI.Unmarshal(resolved, data, &out)
			return
		}
		// A fresh API per input sidesteps the codec cache collision tracked as resolve-cache/default-then-skip.
		assertResolvedRoundTrip(t, fuzzLimits.Freeze(), resolved, reader, data)
	})
}

// assertResolvedRoundTrip checks a value read through resolved encodes with reader and reads back unchanged.
func assertResolvedRoundTrip(t *testing.T, api avro.API, resolved, reader avro.Schema, data []byte) {
	t.Helper()

	v, err := fuzzDecode(api, resolved)(data)
	if err != nil {
		return
	}
	encoded, err := api.Marshal(reader, v)
	if err != nil {
		t.Fatalf("encode resolved value %#v with reader schema: %v", v, err)
	}
	got, err := fuzzDecode(api, reader)(encoded)
	if err != nil {
		t.Fatalf("decode reader encoding %x: %v", encoded, err)
	}
	if !fuzzcheck.Same(v, got) {
		t.Fatalf("resolved value changed through the reader schema:\nresolved: %#v\nreader:   %#v", v, got)
	}
}
