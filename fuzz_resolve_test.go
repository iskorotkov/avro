package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

// FuzzSchemaResolve resolves fuzzed reader/writer schemas and decodes data, and stays out of fuzz.yml until the unbounded-count OOM fix from fix/decoder-hardening lands.
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

	f.Fuzz(func(_ *testing.T, readerSchema, writerSchema string, data []byte) {
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
		var out any
		_ = avro.Unmarshal(resolved, data, &out)
	})
}
