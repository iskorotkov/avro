package avro_test

import (
	"errors"
	"math/big"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func pathEncode(b *testing.B, schemaStr string, v any) {
	b.Helper()

	schema := avro.MustParse(schemaStr)
	if _, err := avro.Marshal(schema, v); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := avro.Marshal(schema, v); err != nil {
			b.Fatal(err)
		}
	}
}

// pathDecode encodes enc once and then repeatedly decodes it into a fresh value made by mk.
func pathDecode(b *testing.B, schemaStr string, enc any, mk func() any) {
	b.Helper()

	schema := avro.MustParse(schemaStr)
	data, err := avro.Marshal(schema, enc)
	if err != nil {
		b.Fatal(err)
	}
	if err := avro.Unmarshal(schema, data, mk()); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := avro.Unmarshal(schema, data, mk()); err != nil {
			b.Fatal(err)
		}
	}
}

// any-heavy values.

const pathAnySchema = `{"type":"record","name":"anyrec","fields":[
	{"name":"a","type":"long"},
	{"name":"b","type":"string"},
	{"name":"c","type":{"type":"array","items":"long"}},
	{"name":"d","type":{"type":"map","values":"string"}}
]}`

type pathAnyRec struct {
	A any `avro:"a"`
	B any `avro:"b"`
	C any `avro:"c"`
	D any `avro:"d"`
}

func pathAnyValue() pathAnyRec {
	return pathAnyRec{
		A: int64(42),
		B: "hello",
		C: []any{int64(1), int64(2), int64(3), int64(4)},
		D: map[string]any{"x": "1", "y": "2", "z": "3"},
	}
}

func BenchmarkPathAny(b *testing.B) {
	b.Run("encode-record", func(b *testing.B) { pathEncode(b, pathAnySchema, pathAnyValue()) })
	b.Run("encode-map-any", func(b *testing.B) {
		pathEncode(b, pathAnySchema, map[string]any{
			"a": int64(42), "b": "hello",
			"c": []any{int64(1), int64(2), int64(3), int64(4)},
			"d": map[string]any{"x": "1", "y": "2", "z": "3"},
		})
	})
	b.Run("encode-slice-any", func(b *testing.B) {
		pathEncode(b, `{"type":"array","items":"long"}`, []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8)})
	})
	b.Run("encode-map-of-any", func(b *testing.B) {
		pathEncode(b, `{"type":"map","values":"string"}`, map[string]any{"a": "1", "b": "2", "c": "3", "d": "4"})
	})
	b.Run("decode-record-into-any", func(b *testing.B) {
		pathDecode(b, pathAnySchema, pathAnyValue(), func() any { return new(any) })
	})
	b.Run("decode-record-any-fields", func(b *testing.B) {
		pathDecode(b, pathAnySchema, pathAnyValue(), func() any { return new(pathAnyRec) })
	})
	b.Run("decode-array-into-any", func(b *testing.B) {
		pathDecode(b, `{"type":"array","items":"long"}`, []int64{1, 2, 3, 4, 5, 6, 7, 8}, func() any { return new(any) })
	})
	b.Run("decode-map-into-any", func(b *testing.B) {
		pathDecode(b, `{"type":"map","values":"string"}`, map[string]string{"a": "1", "b": "2", "c": "3"}, func() any { return new(any) })
	})
}

// top-level pointer-shaped values.

type pathPoint struct {
	X int64 `avro:"x"`
	Y int64 `avro:"y"`
}

type pathOnePtr struct {
	P *int64 `avro:"p"`
}

type pathOnePtrStr struct {
	S *string `avro:"s"`
}

type pathText struct{ V int64 }

func (t *pathText) MarshalText() ([]byte, error) {
	return []byte("text-value"), nil
}

const (
	pathPointSchema = `{"type":"record","name":"point","fields":[{"name":"x","type":"long"},{"name":"y","type":"long"}]}`
	pathOnePtrSchm  = `{"type":"record","name":"oneptr","fields":[{"name":"p","type":["null","long"]}]}`
	pathOnePtrStrSc = `{"type":"record","name":"oneptrstr","fields":[{"name":"s","type":["null","string"]}]}`
)

func BenchmarkPathTopLevelPointerShaped(b *testing.B) {
	n := int64(7)
	s := "pointer"

	b.Run("map", func(b *testing.B) {
		pathEncode(b, `{"type":"map","values":"long"}`, map[string]int64{"a": 1, "b": 2, "c": 3})
	})
	b.Run("ptr-struct", func(b *testing.B) {
		pathEncode(b, pathPointSchema, &pathPoint{X: 1, Y: 2})
	})
	b.Run("struct-one-ptr-field", func(b *testing.B) {
		pathEncode(b, pathOnePtrSchm, pathOnePtr{P: &n})
	})
	b.Run("struct-one-ptr-str-field", func(b *testing.B) {
		pathEncode(b, pathOnePtrStrSc, pathOnePtrStr{S: &s})
	})
	b.Run("ptr-to-ptr-field-struct", func(b *testing.B) {
		pathEncode(b, pathOnePtrSchm, &pathOnePtr{P: &n})
	})
	b.Run("slice-of-ptr-marshaltext-string", func(b *testing.B) {
		pathEncode(b, `{"type":"array","items":"string"}`, []*pathText{{V: 1}, {V: 2}, {V: 3}})
	})
	b.Run("ptr-ptr-recv-marshaltext-string", func(b *testing.B) {
		pathEncode(b, `"string"`, &pathText{V: 1})
	})
}

// defaults missing from the Go struct.

const pathDefaultsSchema = `{"type":"record","name":"defs","fields":[
	{"name":"id","type":"long"},
	{"name":"i","type":"int","default":5},
	{"name":"s","type":"string","default":"dflt"},
	{"name":"f","type":"double","default":1.5},
	{"name":"b","type":"boolean","default":true},
	{"name":"n","type":["null","string"],"default":null},
	{"name":"arr","type":{"type":"array","items":"int"},"default":[1,2,3]},
	{"name":"m","type":{"type":"map","values":"long"},"default":{"k":1}},
	{"name":"by","type":"bytes","default":"abc"},
	{"name":"rec","type":{"type":"record","name":"inner","fields":[{"name":"q","type":"long"}]},"default":{"q":9}}
]}`

type pathDefaultsRec struct {
	ID int64 `avro:"id"`
}

func BenchmarkPathDefaults(b *testing.B) {
	b.Run("encode", func(b *testing.B) { pathEncode(b, pathDefaultsSchema, pathDefaultsRec{ID: 1}) })
	b.Run("encode-ptr", func(b *testing.B) { pathEncode(b, pathDefaultsSchema, &pathDefaultsRec{ID: 1}) })
}

// nullable unions.

type pathNullRec struct {
	S *string    `avro:"s"`
	P *pathPoint `avro:"p"`
	A []int      `avro:"a"`
}

const pathNullSchema = `{"type":"record","name":"nullrec","fields":[
	{"name":"s","type":["null","string"]},
	{"name":"p","type":["null",{"type":"record","name":"pt","fields":[{"name":"x","type":"long"},{"name":"y","type":"long"}]}]},
	{"name":"a","type":["null",{"type":"array","items":"int"}]}
]}`

func BenchmarkPathNullableUnion(b *testing.B) {
	s := "value"
	full := pathNullRec{S: &s, P: &pathPoint{X: 1, Y: 2}, A: []int{1, 2, 3}}

	b.Run("encode-set", func(b *testing.B) { pathEncode(b, pathNullSchema, full) })
	b.Run("encode-nil", func(b *testing.B) { pathEncode(b, pathNullSchema, pathNullRec{}) })
	b.Run("encode-top-ptr-string", func(b *testing.B) { pathEncode(b, `["null","string"]`, &s) })
	b.Run("encode-top-slice", func(b *testing.B) {
		pathEncode(b, `["null",{"type":"array","items":"int"}]`, []int{1, 2, 3})
	})
	b.Run("decode-set", func(b *testing.B) {
		pathDecode(b, pathNullSchema, full, func() any { return new(pathNullRec) })
	})
	b.Run("decode-nil", func(b *testing.B) {
		pathDecode(b, pathNullSchema, pathNullRec{}, func() any { return new(pathNullRec) })
	})
}

// record schema from maps.

type pathKey string

const pathMapRecSchema = `{"type":"record","name":"maprec","fields":[
	{"name":"a","type":"long"},
	{"name":"b","type":"string"},
	{"name":"c","type":{"type":"array","items":"int"}}
]}`

func BenchmarkPathRecordFromMap(b *testing.B) {
	b.Run("map-string-any", func(b *testing.B) {
		pathEncode(b, pathMapRecSchema, map[string]any{"a": int64(1), "b": "two", "c": []int{1, 2, 3}})
	})
	b.Run("map-named-key-any", func(b *testing.B) {
		pathEncode(b, pathMapRecSchema, map[pathKey]any{"a": int64(1), "b": "two", "c": []int{1, 2, 3}})
	})
}

// map decode into fresh maps.

type pathRecVal struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type pathEnumTxt int

func (e *pathEnumTxt) UnmarshalText(data []byte) error {
	switch string(data) {
	case "foo":
		*e = 0
	case "bar":
		*e = 1
	case "baz":
		*e = 2
	default:
		return errors.New("unknown symbol")
	}
	return nil
}

func (e *pathEnumTxt) MarshalText() ([]byte, error) {
	return []byte([]string{"foo", "bar", "baz"}[*e]), nil
}

func BenchmarkPathMapDecode(b *testing.B) {
	const recSchema = `{"type":"map","values":{"type":"record","name":"rv","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`

	b.Run("struct-values", func(b *testing.B) {
		pathDecode(b, recSchema, map[string]pathRecVal{"a": {1, "x"}, "b": {2, "y"}, "c": {3, "z"}},
			func() any { return new(map[string]pathRecVal) })
	})
	b.Run("ptr-struct-values", func(b *testing.B) {
		pathDecode(b, recSchema, map[string]pathRecVal{"a": {1, "x"}, "b": {2, "y"}, "c": {3, "z"}},
			func() any { return new(map[string]*pathRecVal) })
	})
	b.Run("bytes-values", func(b *testing.B) {
		pathDecode(b, `{"type":"map","values":"bytes"}`, map[string][]byte{"a": {1, 2, 3}, "b": {4, 5}, "c": {6}},
			func() any { return new(map[string][]byte) })
	})
	b.Run("map-of-map", func(b *testing.B) {
		pathDecode(b, `{"type":"map","values":{"type":"map","values":"long"}}`,
			map[string]map[string]int64{"a": {"x": 1, "y": 2}, "b": {"z": 3}},
			func() any { return new(map[string]map[string]int64) })
	})
	b.Run("enum-unmarshaltext-values", func(b *testing.B) {
		pathDecode(b, `{"type":"map","values":{"type":"enum","name":"e","symbols":["foo","bar","baz"]}}`,
			map[string]pathEnumTxt{"a": 0, "b": 1, "c": 2},
			func() any { return new(map[string]pathEnumTxt) })
	})
}

// fixed.

func BenchmarkPathFixed(b *testing.B) {
	const arr = `{"type":"fixed","name":"f16","size":16}`
	const dec = `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`

	in := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rat := big.NewRat(12345, 100)

	b.Run("encode-array16", func(b *testing.B) { pathEncode(b, arr, in) })
	b.Run("decode-array16", func(b *testing.B) {
		pathDecode(b, arr, in, func() any { return new([16]byte) })
	})
	b.Run("encode-decimal-rat", func(b *testing.B) { pathEncode(b, dec, rat) })
	b.Run("decode-decimal-rat", func(b *testing.B) {
		pathDecode(b, dec, rat, func() any { return new(*big.Rat) })
	})
}

// bytes and string scalars.

type pathScalarRec struct {
	S  string `avro:"s"`
	By []byte `avro:"by"`
	N  int64  `avro:"n"`
}

const pathScalarSchema = `{"type":"record","name":"scalars","fields":[
	{"name":"s","type":"string"},
	{"name":"by","type":"bytes"},
	{"name":"n","type":"long"}
]}`

func BenchmarkPathScalars(b *testing.B) {
	rec := pathScalarRec{S: "some string value", By: []byte("some bytes value"), N: 99}

	b.Run("encode-string", func(b *testing.B) { pathEncode(b, `"string"`, "some string value") })
	b.Run("decode-string", func(b *testing.B) {
		pathDecode(b, `"string"`, "some string value", func() any { return new(string) })
	})
	b.Run("encode-bytes", func(b *testing.B) { pathEncode(b, `"bytes"`, []byte("some bytes value")) })
	b.Run("decode-bytes", func(b *testing.B) {
		pathDecode(b, `"bytes"`, []byte("some bytes value"), func() any { return new([]byte) })
	})
	b.Run("encode-record", func(b *testing.B) { pathEncode(b, pathScalarSchema, rec) })
	b.Run("decode-record", func(b *testing.B) {
		pathDecode(b, pathScalarSchema, rec, func() any { return new(pathScalarRec) })
	})
}

// dereference of pointer fields.

type pathPtrRec struct {
	N *int64     `avro:"n"`
	S *string    `avro:"s"`
	P *pathPoint `avro:"p"`
}

const pathPtrSchema = `{"type":"record","name":"ptrrec","fields":[
	{"name":"n","type":["null","long"]},
	{"name":"s","type":["null","string"]},
	{"name":"p","type":["null",{"type":"record","name":"pt","fields":[{"name":"x","type":"long"},{"name":"y","type":"long"}]}]}
]}`

func BenchmarkPathDereference(b *testing.B) {
	n := int64(5)
	s := "deref"
	rec := pathPtrRec{N: &n, S: &s, P: &pathPoint{X: 1, Y: 2}}

	b.Run("encode", func(b *testing.B) { pathEncode(b, pathPtrSchema, rec) })
	b.Run("encode-ptr", func(b *testing.B) { pathEncode(b, pathPtrSchema, &rec) })
	b.Run("decode", func(b *testing.B) {
		pathDecode(b, pathPtrSchema, rec, func() any { return new(pathPtrRec) })
	})
}
