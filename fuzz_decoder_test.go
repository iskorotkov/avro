package avro_test

import (
	"bytes"
	"fmt"
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

const fuzzTreeSchema = `{
	"type": "record",
	"name": "Tree",
	"namespace": "fuzz",
	"fields": [
		{"name": "v",        "type": "long"},
		{"name": "children", "type": {"type": "array", "items": "Tree"}},
		{"name": "next",     "type": ["null", "Tree"]}
	]
}`

type fuzzTree struct {
	V        int64      `avro:"v"`
	Children []fuzzTree `avro:"children"`
	Next     *fuzzTree  `avro:"next"`
}

// fuzzTarget is a schema with seed values and the typed Go value its data decodes into.
type fuzzTarget struct {
	schema   avro.Schema
	seeds    []any
	newTyped func() any
}

func fuzzRecordTarget() fuzzTarget {
	var seeds []any
	for _, s := range fuzzSeeds() {
		seeds = append(seeds, s)
	}
	return fuzzTarget{schema: avro.MustParse(fuzzDecoderSchema), seeds: seeds, newTyped: func() any { return &fuzzRecord{} }}
}

func fuzzTreeTarget() fuzzTarget {
	seeds := []any{
		fuzzTree{},
		fuzzTree{V: 1, Children: []fuzzTree{{V: 2}, {V: 3, Next: &fuzzTree{V: 4}}}},
		fuzzTree{V: -1, Next: &fuzzTree{Next: &fuzzTree{Children: []fuzzTree{{}}}}},
	}
	return fuzzTarget{schema: avro.MustParse(fuzzTreeSchema), seeds: seeds, newTyped: func() any { return &fuzzTree{} }}
}

func FuzzDecoder(f *testing.F) {
	fuzzDecoder(f, fuzzLimitedConfig(), fuzzRecordTarget())
}

// FuzzDecoderDefaultConfig fuzzes the limits users get without any configuration.
func FuzzDecoderDefaultConfig(f *testing.F) {
	fuzzDecoder(f, avro.Config{}.Freeze(), fuzzRecordTarget())
}

// FuzzDecoderRecursive fuzzes a recursive record that nests through arrays and unions.
func FuzzDecoderRecursive(f *testing.F) {
	fuzzDecoder(f, fuzzLimitedConfig(), fuzzTreeTarget())
}

func fuzzLimitedConfig() avro.API {
	return avro.Config{
		MaxByteSliceSize:  1 << 20,
		MaxSliceAllocSize: 1 << 22,
		MaxMapAllocSize:   1 << 20,
		MaxDepth:          64,
	}.Freeze()
}

func fuzzDecoder(f *testing.F, api avro.API, target fuzzTarget) {
	f.Helper()

	for _, s := range target.seeds {
		data, err := api.Marshal(target.schema, s)
		if err != nil {
			f.Fatalf("marshal seed: %v", err)
		}
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<24 {
			return
		}
		checkDecodeAgreement(t, api, target, decodeAllWays(api, target, data))
	})
}

func fuzzSeeds() []fuzzRecord {
	return []fuzzRecord{
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
}

// decodeOutcome is the value and error one decode path produced.
type decodeOutcome struct {
	way   string
	value any
	err   error
}

// decodeAllWays decodes data into a typed value, a generic value, a stream and ReadNext; skipping is always last.
func decodeAllWays(api avro.API, target fuzzTarget, data []byte) []decodeOutcome {
	typed := target.newTyped()
	typedErr := api.Unmarshal(target.schema, data, typed)

	var generic any
	genericErr := api.Unmarshal(target.schema, data, &generic)

	stream := target.newTyped()
	streamErr := api.NewDecoder(target.schema, bytes.NewReader(data)).Decode(stream)

	r := avro.NewReader(nil, 0, avro.WithReaderConfig(api)).Reset(data)
	next := r.ReadNext(target.schema)

	return []decodeOutcome{
		{way: "typed", value: typed, err: typedErr},
		{way: "generic", value: generic, err: genericErr},
		{way: "stream", value: stream, err: streamErr},
		{way: "ReadNext", value: next, err: r.Error},
		{way: "skip", err: api.Unmarshal(target.schema, data, &struct{}{})},
	}
}

// checkDecodeAgreement checks that all decode paths agree on success and that successful values re-encode identically.
func checkDecodeAgreement(t *testing.T, api avro.API, target fuzzTarget, outcomes []decodeOutcome) {
	t.Helper()

	// Skipping trusts block byte sizes that decoders ignore, so it is only checked on canonical bytes below.
	decoded := outcomes[:len(outcomes)-1]
	for _, o := range decoded[1:] {
		if (o.err == nil) != (decoded[0].err == nil) {
			t.Fatalf("%s error %v disagrees with %s error %v", o.way, o.err, decoded[0].way, decoded[0].err)
		}
	}
	if decoded[0].err != nil {
		return
	}
	want := reencode(t, api, target.schema, decoded[0])
	for _, o := range decoded[1:] {
		if got := reencode(t, api, target.schema, o); got.value != want.value {
			t.Fatalf("%s re-encodes to %s, %s to %s", o.way, got.value, decoded[0].way, want.value)
		}
	}

	// Re-encoding is canonical up to map order, so decoding it again must re-encode to the same value.
	again := target.newTyped()
	if err := api.Unmarshal(target.schema, want.data, again); err != nil {
		t.Fatalf("decode re-encoded %x: %v", want.data, err)
	}
	if got := reencode(t, api, target.schema, decodeOutcome{way: "round trip", value: again}); got.value != want.value {
		t.Fatalf("round trip re-encodes to %s, want %s", got.value, want.value)
	}
	if err := api.Unmarshal(target.schema, want.data, &struct{}{}); err != nil {
		t.Fatalf("skip re-encoded %x: %v", want.data, err)
	}
}

// reencoding is re-encoded data with a printed form that, unlike the bytes, does not depend on map iteration order.
type reencoding struct {
	data  []byte
	value string
}

func reencode(t *testing.T, api avro.API, schema avro.Schema, o decodeOutcome) reencoding {
	t.Helper()

	data, err := api.Marshal(schema, o.value)
	if err != nil {
		t.Fatalf("re-encode %s value: %v", o.way, err)
	}
	var generic any
	if err = api.Unmarshal(schema, data, &generic); err != nil {
		t.Fatalf("decode %s re-encoding %x: %v", o.way, data, err)
	}
	// Printing sorts map keys, so equal values print identically.
	return reencoding{data: data, value: fmt.Sprintf("%#v", generic)}
}
