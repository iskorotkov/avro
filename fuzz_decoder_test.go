package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
)

const fuzzDecoderSchema = `{
	"type": "record",
	"name": "FuzzRecord",
	"namespace": "fuzz",
	"fields": [
		{"name": "i",  "type": "int"},
		{"name": "l",  "type": "long"},
		{"name": "f",  "type": "float"},
		{"name": "d",  "type": "double"},
		{"name": "b",  "type": "boolean"},
		{"name": "s",  "type": "string"},
		{"name": "by", "type": "bytes"},
		{"name": "u",  "type": ["null", "int", "string"]},
		{"name": "a",  "type": {"type": "array", "items": "long"}},
		{"name": "m",  "type": {"type": "map",   "values": "string"}},
		{"name": "e",  "type": {"type": "enum",  "name": "E", "symbols": ["A", "B", "C"]}},
		{"name": "x",  "type": {"type": "fixed", "name": "F", "size": 4}}
	]
}`

type fuzzRecord struct {
	I  int32             `avro:"i"`
	L  int64             `avro:"l"`
	F  float32           `avro:"f"`
	D  float64           `avro:"d"`
	B  bool              `avro:"b"`
	S  string            `avro:"s"`
	By []byte            `avro:"by"`
	U  any               `avro:"u"`
	A  []int64           `avro:"a"`
	M  map[string]string `avro:"m"`
	E  string            `avro:"e"`
	X  [4]byte           `avro:"x"`
}

func FuzzDecoder(f *testing.F) {
	avro.DefaultConfig = avro.Config{
		MaxByteSliceSize:  1 << 20,
		MaxSliceAllocSize: 1 << 22,
		MaxMapAllocSize:   1 << 20,
	}.Freeze()
	defer ConfigTeardown()

	schema, err := avro.Parse(fuzzDecoderSchema)
	if err != nil {
		f.Fatalf("parse fuzz schema: %v", err)
	}

	seeds := []fuzzRecord{
		{E: "A"},
		{
			I: 1, L: 2, F: 3, D: 4, B: true, S: "hello",
			By: []byte{0x00, 0x01}, U: map[string]any{"int": int32(7)},
			A: []int64{1, 2, 3}, M: map[string]string{"k": "v"},
			E: "A", X: [4]byte{1, 2, 3, 4},
		},
		{
			I: -1 << 30, L: -1 << 60, F: -1.5, D: -2.5, B: false,
			S: "", By: nil, U: nil, A: nil, M: nil,
			E: "C", X: [4]byte{},
		},
	}
	for _, s := range seeds {
		data, err := avro.Marshal(schema, s)
		if err != nil {
			f.Fatalf("marshal seed: %v", err)
		}
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff})

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<24 {
			return
		}
		var out fuzzRecord
		_ = avro.Unmarshal(schema, data, &out)
		var generic any
		_ = avro.Unmarshal(schema, data, &generic)
	})
}
