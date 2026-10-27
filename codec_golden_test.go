package avro_test

// Goldens hold reflect2-era encoder output; regenerate them with `make golden`, never with -golden.update on this branch.

import (
	"bytes"
	"flag"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var goldenUpdate = flag.Bool("golden.update", false, "write golden files instead of comparing")

type goldenAny any

type goldenInner struct {
	A int    `avro:"a"`
	B string `avro:"b"`
}

type goldenEmbedValue struct {
	goldenInner
	C int64 `avro:"c"`
}

type goldenEmbedPtr struct {
	*goldenInner
	C int64 `avro:"c"`
}

type GoldenExportedInner struct {
	A int    `avro:"a"`
	B string `avro:"b"`
}

type goldenEmbedExported struct {
	GoldenExportedInner
	C int64 `avro:"c"`
}

type goldenEmbedExportedPtr struct {
	*GoldenExportedInner
	C int64 `avro:"c"`
}

type goldenUnexported struct {
	A      int `avro:"a"`
	hidden string
	Z      string `avro:"z"`
	other  int
}

type goldenPtrFields struct {
	S *string      `avro:"s"`
	I *int         `avro:"i"`
	R *goldenInner `avro:"r"`
}

type goldenIfaceFields struct {
	A any       `avro:"a"`
	N goldenAny `avro:"n"`
}

type goldenStringer interface{ String() string }

type goldenStr string

func (s goldenStr) String() string { return string(s) }

type goldenStringerRec struct {
	V goldenStringer `avro:"v"`
}

type goldenKey string

func (k goldenKey) MarshalText() ([]byte, error) { return []byte("k:" + string(k)), nil }

type goldenPtrKey struct{ N string }

func (k *goldenPtrKey) MarshalText() ([]byte, error) { return []byte("pk:" + k.N), nil }

type goldenAppendKey struct{ N string }

func (k goldenAppendKey) AppendText(b []byte) ([]byte, error) {
	return append(b, "ak:"+k.N...), nil
}

type goldenPtrAppendKey struct{ N string }

func (k *goldenPtrAppendKey) AppendText(b []byte) ([]byte, error) {
	return append(b, "pak:"+k.N...), nil
}

type goldenNamedStr string

type goldenEnum int

func (e goldenEnum) MarshalText() ([]byte, error) {
	return []byte([]string{"ZERO", "ONE", "TWO"}[e]), nil
}

type goldenPtrEnum int

func (e *goldenPtrEnum) MarshalText() ([]byte, error) {
	return []byte([]string{"ZERO", "ONE", "TWO"}[*e]), nil
}

type goldenAppendEnum int

func (e goldenAppendEnum) AppendText(b []byte) ([]byte, error) {
	return append(b, []string{"ZERO", "ONE", "TWO"}[e]...), nil
}

type goldenEnumRec struct {
	E  goldenEnum       `avro:"e"`
	P  goldenPtrEnum    `avro:"p"`
	A  goldenAppendEnum `avro:"a"`
	S  string           `avro:"s"`
	PE *goldenEnum      `avro:"pe"`
}

type goldenEmpty struct{}

type goldenDefaultsMissing struct {
	Keep string `avro:"keep"`
}

type goldenTimeRec struct {
	T  time.Time     `avro:"t"`
	Tm time.Time     `avro:"tm"`
	Tu time.Time     `avro:"tu"`
	D  time.Duration `avro:"d"`
	Dt time.Time     `avro:"dt"`
	Lt time.Time     `avro:"lt"`
}

type goldenFixedRec struct {
	F [4]byte `avro:"f"`
	P [2]byte `avro:"p"`
}

type goldenDecimalRec struct {
	D  *big.Rat `avro:"d"`
	Fx *big.Rat `avro:"fx"`
}

type goldenArrayRec struct {
	Items []goldenInner    `avro:"items"`
	Ptrs  []*goldenInner   `avro:"ptrs"`
	Ints  []int32          `avro:"ints"`
	Nest  [][]string       `avro:"nest"`
	Maps  []map[string]int `avro:"maps"`
}

type goldenNestedMaps struct {
	M map[string]map[string][]int `avro:"m"`
	N map[string]goldenInner      `avro:"n"`
	P map[string]*goldenInner     `avro:"p"`
}

type goldenUnionRec struct {
	N   *string        `avro:"n"`
	NI  *int           `avro:"ni"`
	NR  *goldenInner   `avro:"nr"`
	A   any            `avro:"a"`
	M   map[string]any `avro:"m"`
	NM  map[string]any `avro:"nm"`
	Arr []any          `avro:"arr"`
}

type goldenBytesRec struct {
	B  []byte         `avro:"b"`
	S  string         `avro:"s"`
	U  string         `avro:"u"`
	Nb []byte         `avro:"nb"`
	N  goldenNamedStr `avro:"n"`
}

type goldenPrimRec struct {
	I   int     `avro:"i"`
	I8  int8    `avro:"i8"`
	I16 int16   `avro:"i16"`
	I32 int32   `avro:"i32"`
	I64 int64   `avro:"i64"`
	U8  uint8   `avro:"u8"`
	U16 uint16  `avro:"u16"`
	U32 uint32  `avro:"u32"`
	F32 float32 `avro:"f32"`
	F64 float64 `avro:"f64"`
	B   bool    `avro:"b"`
}

type goldenTaggedRec struct {
	First  string `avro:"first_name"`
	Second int    `avro:"second_name"`
	Plain  bool
}

type goldenUnionOne struct {
	X int `avro:"x"`
}

type goldenUnionTwo struct {
	Y string `avro:"y"`
}

type goldenNode struct {
	V    int         `avro:"v"`
	Next *goldenNode `avro:"next"`
}

type goldenCase struct {
	name        string
	schema      string
	value       any
	noRoundTrip bool
}

func goldenAPI() avro.API {
	api := avro.Config{}.Freeze()
	api.Register("inner", goldenInner{})
	api.Register("one", goldenUnionOne{})
	api.Register("two", goldenUnionTwo{})
	return api
}

func goldenCases() []goldenCase {
	inner := `{"type":"record","name":"inner","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	abc := `{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"long"}]}`
	enum := `{"type":"enum","name":"en","symbols":["ZERO","ONE","TWO"]}`
	arrays := `{"type":"record","name":"r","fields":[{"name":"items","type":{"type":"array","items":` + inner + `}},{"name":"ptrs","type":{"type":"array","items":"inner"}},{"name":"ints","type":{"type":"array","items":"int"}},{"name":"nest","type":{"type":"array","items":{"type":"array","items":"string"}}},{"name":"maps","type":{"type":"array","items":{"type":"map","values":"int"}}}]}`
	ts := time.Date(2020, 1, 2, 3, 4, 5, 123456000, time.UTC)
	e := goldenEnum(2)
	pe := goldenPtrEnum(1)
	pp := &goldenInner{A: 4, B: "pp"}

	return []goldenCase{
		{name: "embed-value", schema: abc, value: goldenEmbedValue{goldenInner: goldenInner{A: 1, B: "x"}, C: 9}, noRoundTrip: true},
		{name: "embed-ptr-nonnil", schema: abc, value: goldenEmbedPtr{goldenInner: &goldenInner{A: 1, B: "x"}, C: 9}, noRoundTrip: true},
		{name: "embed-exported-value", schema: abc, value: goldenEmbedExported{GoldenExportedInner: GoldenExportedInner{A: 3, B: "y"}, C: -9}},
		{name: "embed-exported-ptr", schema: abc, value: goldenEmbedExportedPtr{GoldenExportedInner: &GoldenExportedInner{A: 3, B: "y"}, C: -9}},
		{name: "embed-exported-ptr-in-ptr", schema: abc, value: &goldenEmbedExportedPtr{GoldenExportedInner: &GoldenExportedInner{A: 3, B: "y"}, C: 7}},
		{
			name: "unexported-ignored", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"z","type":"string"}]}`,
			value: goldenUnexported{A: 5, hidden: "h", Z: "zed", other: 1}, noRoundTrip: true,
		},
		{
			name: "tagged-and-untagged", schema: `{"type":"record","name":"r","fields":[{"name":"first_name","type":"string"},{"name":"second_name","type":"int"},{"name":"Plain","type":"boolean"}]}`,
			value: goldenTaggedRec{First: "f", Second: 2, Plain: true},
		},
		{
			name: "ptr-fields-nil", schema: `{"type":"record","name":"r","fields":[{"name":"s","type":["null","string"]},{"name":"i","type":["null","int"]},{"name":"r","type":["null",` + inner + `]}]}`,
			value: goldenPtrFields{},
		},
		{
			name: "ptr-fields-set", schema: `{"type":"record","name":"r","fields":[{"name":"s","type":["null","string"]},{"name":"i","type":["null","int"]},{"name":"r","type":["null",` + inner + `]}]}`,
			value: goldenPtrFields{S: new("s"), I: new(7), R: &goldenInner{A: 1, B: "b"}},
		},
		{name: "ptr-to-ptr-record", schema: inner, value: &pp, noRoundTrip: true},
		{
			name: "iface-fields-union", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":["null","string","int"]},{"name":"n","type":["null","string","long"]}]}`,
			value: goldenIfaceFields{A: "hello", N: int64(5)}, noRoundTrip: true,
		},
		{
			name: "iface-fields-nil", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":["null","string"]},{"name":"n","type":["null","string"]}]}`,
			value: goldenIfaceFields{}, noRoundTrip: true,
		},
		{
			name: "iface-fields-concrete-schema", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":"string"},{"name":"n","type":"int"}]}`,
			value: goldenIfaceFields{A: "plain", N: 11}, noRoundTrip: true,
		},
		{
			name: "iface-fields-record", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":["null",` + inner + `]},{"name":"n","type":["null","inner"]}]}`,
			value: goldenIfaceFields{A: goldenInner{A: 1, B: "q"}, N: &goldenInner{A: 2, B: "w"}}, noRoundTrip: true,
		},
		{
			name: "iface-nonempty", schema: `{"type":"record","name":"r","fields":[{"name":"v","type":"string"}]}`,
			value: goldenStringerRec{V: goldenStr("stringer")}, noRoundTrip: true,
		},
		{name: "map-string-keys", schema: `{"type":"map","values":"int"}`, value: map[string]int{"one": 1}},
		{name: "map-named-string-keys", schema: `{"type":"map","values":"int"}`, value: map[goldenNamedStr]int{"named": 2}, noRoundTrip: true},
		{name: "map-text-marshaler-keys", schema: `{"type":"map","values":"string"}`, value: map[goldenKey]string{"a": "v"}, noRoundTrip: true},
		{name: "map-ptr-receiver-text-keys", schema: `{"type":"map","values":"string"}`, value: map[*goldenPtrKey]string{{N: "a"}: "v"}, noRoundTrip: true},
		{name: "map-text-appender-keys", schema: `{"type":"map","values":"string"}`, value: map[goldenAppendKey]string{{N: "a"}: "v"}, noRoundTrip: true},
		{name: "map-ptr-appender-keys", schema: `{"type":"map","values":"string"}`, value: map[*goldenPtrAppendKey]string{{N: "a"}: "v"}, noRoundTrip: true},
		{name: "map-time-keys", schema: `{"type":"map","values":"int"}`, value: map[time.Time]int{ts: 1}, noRoundTrip: true},
		{name: "map-ptr-time-keys", schema: `{"type":"map","values":"int"}`, value: map[*time.Time]int{&ts: 1}, noRoundTrip: true},
		{name: "map-empty", schema: `{"type":"map","values":"int"}`, value: map[string]int{}},
		{name: "map-nil", schema: `{"type":"map","values":"int"}`, value: map[string]int(nil), noRoundTrip: true},
		{name: "map-any-values", schema: `{"type":"map","values":["null","string","int"]}`, value: map[string]any{"k": "v"}, noRoundTrip: true},
		{name: "map-record-values", schema: `{"type":"map","values":` + inner + `}`, value: map[string]goldenInner{"k": {A: 1, B: "b"}}},
		{name: "map-ptr-record-values", schema: `{"type":"map","values":` + inner + `}`, value: map[string]*goldenInner{"k": {A: 1, B: "b"}}},
		{name: "map-nullable-ptr-values", schema: `{"type":"map","values":["null","string"]}`, value: map[string]*string{"k": new("z")}},
		{
			name: "nested-maps", schema: `{"type":"record","name":"r","fields":[{"name":"m","type":{"type":"map","values":{"type":"map","values":{"type":"array","items":"int"}}}},{"name":"n","type":{"type":"map","values":` + inner + `}},{"name":"p","type":{"type":"map","values":"inner"}}]}`,
			value: goldenNestedMaps{M: map[string]map[string][]int{"x": {"y": {1, 2, 3}}}, N: map[string]goldenInner{"n": {A: 1, B: "n"}}, P: map[string]*goldenInner{"p": {A: 2, B: "p"}}},
		},
		{
			name: "enum-text-marshaler", schema: `{"type":"record","name":"r","fields":[{"name":"e","type":` + enum + `},{"name":"p","type":"en"},{"name":"a","type":"en"},{"name":"s","type":"en"},{"name":"pe","type":"en"}]}`,
			value: goldenEnumRec{E: 2, P: 1, A: 1, S: "TWO", PE: &e}, noRoundTrip: true,
		},
		{name: "enum-ptr-marshaler", schema: enum, value: &pe, noRoundTrip: true},
		{name: "enum-string", schema: enum, value: "ONE"},
		{name: "enum-named-string", schema: enum, value: goldenNamedStr("TWO")},
		{name: "enum-top-level-marshaler", schema: enum, value: goldenEnum(1), noRoundTrip: true},
		{name: "enum-top-level-appender", schema: enum, value: goldenAppendEnum(2), noRoundTrip: true},
		{
			name: "fixed", schema: `{"type":"record","name":"r","fields":[{"name":"f","type":{"type":"fixed","name":"f4","size":4}},{"name":"p","type":{"type":"fixed","name":"f2","size":2}}]}`,
			value: goldenFixedRec{F: [4]byte{1, 2, 3, 4}, P: [2]byte{9, 8}},
		},
		{name: "fixed-top-level", schema: `{"type":"fixed","name":"f4","size":4}`, value: [4]byte{0xde, 0xad, 0xbe, 0xef}},
		{
			name: "decimal-bytes", schema: `{"type":"record","name":"r","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},{"name":"fx","type":{"type":"fixed","name":"dfx","size":8,"logicalType":"decimal","precision":10,"scale":3}}]}`,
			value: goldenDecimalRec{D: big.NewRat(1734, 5), Fx: big.NewRat(-12345, 1000)},
		},
		{name: "decimal-top-level", schema: `{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}`, value: big.NewRat(-1, 4), noRoundTrip: true},
		{
			name: "logical-times", schema: `{"type":"record","name":"r","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"tm","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"tu","type":{"type":"long","logicalType":"local-timestamp-micros"}},{"name":"d","type":{"type":"int","logicalType":"time-millis"}},{"name":"dt","type":{"type":"int","logicalType":"date"}},{"name":"lt","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
			value: goldenTimeRec{T: ts, Tm: ts, Tu: ts, D: 90 * time.Minute, Dt: ts, Lt: time.Unix(0, 0).UTC()}, noRoundTrip: true,
		},
		{name: "logical-time-micros-duration", schema: `{"type":"long","logicalType":"time-micros"}`, value: 5*time.Hour + 3*time.Microsecond},
		{name: "logical-uuid", schema: `{"type":"string","logicalType":"uuid"}`, value: "123e4567-e89b-12d3-a456-426614174000"},
		{
			name: "bytes-strings", schema: `{"type":"record","name":"r","fields":[{"name":"b","type":"bytes"},{"name":"s","type":"string"},{"name":"u","type":"string"},{"name":"nb","type":["null","bytes"]},{"name":"n","type":"string"}]}`,
			value: goldenBytesRec{B: []byte{0, 1, 255}, S: "", U: "héllo, 世界", N: "named"}, noRoundTrip: true,
		},
		{
			name: "primitives", schema: `{"type":"record","name":"r","fields":[{"name":"i","type":"int"},{"name":"i8","type":"int"},{"name":"i16","type":"int"},{"name":"i32","type":"int"},{"name":"i64","type":"long"},{"name":"u8","type":"int"},{"name":"u16","type":"int"},{"name":"u32","type":"long"},{"name":"f32","type":"float"},{"name":"f64","type":"double"},{"name":"b","type":"boolean"}]}`,
			value: goldenPrimRec{I: -300, I8: -8, I16: 1600, I32: -32, I64: 1 << 40, U8: 200, U16: 60000, U32: 4000000000, F32: 1.5, F64: -2.25, B: true},
		},
		{
			name: "arrays", schema: arrays,
			value: goldenArrayRec{Items: []goldenInner{{A: 1, B: "a"}, {A: 2, B: "b"}}, Ptrs: []*goldenInner{{A: 3, B: "c"}}, Ints: []int32{1, -2, 3}, Nest: [][]string{{"x"}, {"y", "z"}, {}}, Maps: []map[string]int{{"m": 1}}},
		},
		{name: "arrays-empty-nil", schema: arrays, value: goldenArrayRec{Items: []goldenInner{}}, noRoundTrip: true},
		{name: "array-of-structs-top-level", schema: `{"type":"array","items":` + inner + `}`, value: []goldenInner{{A: 1, B: "a"}, {A: 2, B: "b"}}},
		{name: "array-of-fixed", schema: `{"type":"array","items":{"type":"fixed","name":"f2","size":2}}`, value: [][2]byte{{1, 2}, {3, 4}}},
		{
			name: "union-nullable-ptr-nil", schema: `{"type":"record","name":"r","fields":[{"name":"n","type":["null","string"]},{"name":"ni","type":["null","int"]},{"name":"nr","type":["null",` + inner + `]},{"name":"a","type":["null","string"]},{"name":"m","type":["null","int"]},{"name":"nm","type":["null","int"]},{"name":"arr","type":{"type":"array","items":["null","int"]}}]}`,
			value: goldenUnionRec{}, noRoundTrip: true,
		},
		{
			name: "union-nullable-ptr-set", schema: `{"type":"record","name":"r","fields":[{"name":"n","type":["null","string"]},{"name":"ni","type":["int","null"]},{"name":"nr","type":["null",` + inner + `]},{"name":"a","type":["null","string","inner"]},{"name":"m","type":["null","int"]},{"name":"nm","type":["null","int"]},{"name":"arr","type":{"type":"array","items":["null","int","string"]}}]}`,
			value: goldenUnionRec{N: new("n"), NI: new(3), NR: &goldenInner{A: 1, B: "b"}, A: "any", M: map[string]any{"int": 5}, Arr: []any{nil, 1, "s"}}, noRoundTrip: true,
		},
		{name: "union-map-form", schema: `["null","string","int"]`, value: map[string]any{"string": "mapped"}, noRoundTrip: true},
		{name: "union-map-form-record", schema: `["null",` + inner + `,"int"]`, value: map[string]any{"inner": map[string]any{"a": 1, "b": "x"}}, noRoundTrip: true},
		{name: "union-map-form-null", schema: `["null","int"]`, value: map[string]any(nil), noRoundTrip: true},
		{
			name: "union-any-resolved-names", schema: `{"type":"record","name":"top","fields":[{"name":"a","type":["null",{"type":"record","name":"one","fields":[{"name":"x","type":"int"}]},{"type":"record","name":"two","fields":[{"name":"y","type":"string"}]}]},{"name":"b","type":["null","one","two"]}]}`,
			value: map[string]any{"a": goldenUnionOne{X: 4}, "b": goldenUnionTwo{Y: "yy"}}, noRoundTrip: true,
		},
		{name: "union-top-level-any-double", schema: `["null","int","string","double"]`, value: 3.5, noRoundTrip: true},
		{name: "union-top-level-any-nil", schema: `["null","int"]`, value: nil, noRoundTrip: true},
		{name: "union-top-level-int32", schema: `["null","int","long"]`, value: int32(12), noRoundTrip: true},
		{name: "union-top-level-slice", schema: `["null",{"type":"array","items":"int"}]`, value: []int{1, 2}, noRoundTrip: true},
		{name: "union-top-level-ptr-nil", schema: `["null","string"]`, value: (*string)(nil), noRoundTrip: true},
		{
			name: "union-record-ptr-and-value", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":["null",` + inner + `]},{"name":"n","type":["null","inner"]}]}`,
			value: goldenIfaceFields{A: &goldenInner{A: 1, B: "p"}, N: goldenInner{A: 2, B: "v"}}, noRoundTrip: true,
		},
		{name: "record-from-map-string-any", schema: inner, value: map[string]any{"a": 5, "b": "mm"}, noRoundTrip: true},
		{
			name: "defaults-primitives", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"b","type":"boolean","default":true},{"name":"i","type":"int","default":-3},{"name":"l","type":"long","default":123456789012},{"name":"f","type":"float","default":1.5},{"name":"d","type":"double","default":-2.5},{"name":"s","type":"string","default":"dflt"},{"name":"by","type":"bytes","default":"ÿ\u0001"},{"name":"n","type":"null","default":null}]}`,
			value: goldenDefaultsMissing{Keep: "k"}, noRoundTrip: true,
		},
		{
			name: "defaults-complex", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"arr","type":{"type":"array","items":"int"},"default":[1,2,3]},{"name":"m","type":{"type":"map","values":"string"},"default":{"k":"v"}},{"name":"e","type":{"type":"enum","name":"en","symbols":["A","B","C"]},"default":"B"},{"name":"fx","type":{"type":"fixed","name":"fx3","size":3},"default":"abc"},{"name":"rec","type":{"type":"record","name":"sub","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]},"default":{"x":5,"y":"yy"}},{"name":"ea","type":{"type":"array","items":"string"},"default":[]},{"name":"em","type":{"type":"map","values":"int"},"default":{}}]}`,
			value: goldenDefaultsMissing{Keep: "k"}, noRoundTrip: true,
		},
		{
			name: "defaults-unions", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"un","type":["null","string"],"default":null},{"name":"us","type":["string","null"],"default":"first"},{"name":"ui","type":["int","string"],"default":7},{"name":"ua","type":[{"type":"array","items":"int"},"null"],"default":[4,5]}]}`,
			value: goldenDefaultsMissing{Keep: "k"}, noRoundTrip: true,
		},
		{
			name: "defaults-logical", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"},"default":1000},{"name":"dt","type":{"type":"int","logicalType":"date"},"default":18000},{"name":"du","type":{"type":"int","logicalType":"time-millis"},"default":60000},{"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2},"default":"\u0001\u0002"},{"name":"uid","type":{"type":"string","logicalType":"uuid"},"default":"123e4567-e89b-12d3-a456-426614174000"}]}`,
			value: goldenDefaultsMissing{Keep: "k"}, noRoundTrip: true,
		},
		{
			name: "defaults-partial-override", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"A","type":"int","default":42},{"name":"b","type":"string","default":"bee"}]}`,
			value: struct {
				Keep string `avro:"keep"`
				B    string `avro:"b"`
			}{Keep: "k", B: "given"}, noRoundTrip: true,
		},
		{
			name: "defaults-nested-record", schema: `{"type":"record","name":"r","fields":[{"name":"in","type":{"type":"record","name":"in","fields":[{"name":"p","type":"int"},{"name":"q","type":"string","default":"qq"},{"name":"w","type":{"type":"array","items":"long"},"default":[9]}]}}]}`,
			value: struct {
				In struct {
					P int `avro:"p"`
				} `avro:"in"`
			}{In: struct {
				P int `avro:"p"`
			}{P: 1}}, noRoundTrip: true,
		},
		{
			name: "defaults-map-value", schema: `{"type":"record","name":"r","fields":[{"name":"keep","type":"string"},{"name":"extra","type":"int","default":8}]}`,
			value: map[string]any{"keep": "mk"}, noRoundTrip: true,
		},
		{name: "record-empty", schema: `{"type":"record","name":"r","fields":[]}`, value: goldenEmpty{}},
		{
			name: "record-recursive", schema: `{"type":"record","name":"node","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","node"]}]}`,
			value: &goldenNode{V: 1, Next: &goldenNode{V: 2, Next: &goldenNode{V: 3}}},
		},
		{
			name: "ref-named-types", schema: `{"type":"record","name":"r","fields":[{"name":"a","type":` + inner + `},{"name":"b","type":"inner"},{"name":"c","type":{"type":"array","items":"inner"}}]}`,
			value: struct {
				A goldenInner   `avro:"a"`
				B goldenInner   `avro:"b"`
				C []goldenInner `avro:"c"`
			}{A: goldenInner{A: 1, B: "a"}, B: goldenInner{A: 2, B: "b"}, C: []goldenInner{{A: 3, B: "c"}}},
		},
		{name: "ptr-top-level-record", schema: inner, value: &goldenInner{A: 6, B: "ptr"}},
	}
}

func TestGolden(t *testing.T) {
	require.NoError(t, os.MkdirAll(filepath.Join("testdata", "golden"), 0o750))
	api := goldenAPI()

	for _, tt := range goldenCases() {
		t.Run(tt.name, func(t *testing.T) {
			schema := avro.MustParse(tt.schema)

			got, err := api.Marshal(schema, tt.value)
			require.NoError(t, err)

			path := filepath.Join("testdata", "golden", tt.name+".avro")
			if *goldenUpdate {
				require.NoError(t, os.WriteFile(path, got, 0o600))
				return
			}

			want, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, want, got, "got %x want %x", got, want)

			if tt.noRoundTrip {
				return
			}

			ptr := reflect.New(reflect.TypeOf(tt.value))
			require.NoError(t, api.Unmarshal(schema, want, ptr.Interface()))

			again, err := api.Marshal(schema, ptr.Elem().Interface())
			require.NoError(t, err)
			assert.True(t, bytes.Equal(want, again), "round trip got %x want %x", again, want)
		})
	}
}
