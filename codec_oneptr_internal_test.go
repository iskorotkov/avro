package avro

import (
	"strconv"
	"testing"

	"github.com/modern-go/reflect2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retainedText is a pointer-like type whose text method keeps its receiver.
type retainedText struct {
	p *int
}

var retainedTexts []*retainedText

func (e *retainedText) MarshalText() ([]byte, error) {
	retainedTexts = append(retainedTexts, e)
	return []byte("s" + strconv.Itoa(*e.p)), nil
}

type retainedWrap struct {
	E retainedText `avro:"e"`
}

//go:noinline
func clobberStack(n int) int {
	var buf [256]int
	for i := range buf {
		buf[i] = -1
	}
	if n > 0 {
		return clobberStack(n-1) + buf[n]
	}
	return buf[0]
}

// assertRetainedValid checks that the only retained receiver still points at v after the stack is reused.
func assertRetainedValid(t *testing.T, v *int) {
	t.Helper()

	clobberStack(8)

	require.Len(t, retainedTexts, 1)
	assert.Same(t, v, retainedTexts[0].p)
}

func resetRetainedTexts(t *testing.T) {
	t.Helper()

	retainedTexts = nil
	t.Cleanup(func() { retainedTexts = nil })
}

const retainedEnumSchema = `{"type":"enum","name":"e","symbols":["s0","s1","s2","s3"]}`

// recordWithEnumDefault returns a record with an int field a and an enum field e whose default is def.
func recordWithEnumDefault(t *testing.T, def any, action Action) *RecordSchema {
	t.Helper()

	a, err := NewField("a", NewPrimitiveSchema(Int, nil))
	require.NoError(t, err)
	e, err := NewField("e", MustParse(retainedEnumSchema), WithDefault("s0"))
	require.NoError(t, err)
	e.def = def
	e.action = action
	rec, err := NewRecordSchema("r", "", []*Field{a, e})
	require.NoError(t, err)
	return rec
}

func TestEncoder_PointerLikeValueKeepsRetainedReceiverValid(t *testing.T) {
	resetRetainedTexts(t)
	v := 2

	_, err := Config{}.Freeze().Marshal(MustParse(retainedEnumSchema), retainedText{p: &v})
	require.NoError(t, err)

	assertRetainedValid(t, &v)
}

func TestEncoder_PointerLikeStructKeepsRetainedFieldValid(t *testing.T) {
	resetRetainedTexts(t)
	schema := MustParse(`{"type":"record","name":"r","fields":[{"name":"e","type":` + retainedEnumSchema + `}]}`)
	v := 2

	_, err := Config{}.Freeze().Marshal(schema, retainedWrap{E: retainedText{p: &v}})
	require.NoError(t, err)

	assertRetainedValid(t, &v)
}

func TestNewOnePtrEncoder_ArrayOfAddressExposingTypeUsesHeap(t *testing.T) {
	enc := newOnePtrEncoder(reflect2.TypeOf([1]retainedText{}), nil)

	assert.IsType(t, &heapPtrEncoder{}, enc)
}

func TestEncoder_StructDefaultKeepsRetainedReceiverValid(t *testing.T) {
	resetRetainedTexts(t)
	v := 2
	schema := recordWithEnumDefault(t, retainedText{p: &v}, "")

	_, err := Config{}.Freeze().Marshal(schema, struct {
		A int `avro:"a"`
	}{})
	require.NoError(t, err)

	assertRetainedValid(t, &v)
}

func TestEncoder_MapDefaultKeepsRetainedReceiverValid(t *testing.T) {
	resetRetainedTexts(t)
	v := 2
	schema := recordWithEnumDefault(t, retainedText{p: &v}, "")

	_, err := Config{}.Freeze().Marshal(schema, map[string]any{"a": 1})
	require.NoError(t, err)

	assertRetainedValid(t, &v)
}

func TestDecoder_DefaultKeepsRetainedReceiverValid(t *testing.T) {
	resetRetainedTexts(t)
	v := 2
	schema := recordWithEnumDefault(t, retainedText{p: &v}, FieldSetDefault)

	var got struct {
		A int    `avro:"a"`
		E string `avro:"e"`
	}
	err := Config{}.Freeze().Unmarshal(schema, []byte{0x02}, &got)
	require.NoError(t, err)

	assertRetainedValid(t, &v)
	assert.Equal(t, "s2", got.E)
}

func TestEncoder_MapUnionKeepsRetainedReceiverValid(t *testing.T) {
	resetRetainedTexts(t)
	schema := MustParse(`["null",` + retainedEnumSchema + `]`)
	v := 2

	_, err := Config{}.Freeze().Marshal(schema, map[string]any{"e": retainedText{p: &v}})
	require.NoError(t, err)

	assertRetainedValid(t, &v)
}
