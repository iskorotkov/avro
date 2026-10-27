package avro_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const nestedErrSchema = `{"type":"record","name":"outer","fields":[{"name":"inner","type":{"type":"record","name":"inner","fields":[{"name":"b","type":"string"}]}}]}`

type errInner struct {
	B string `avro:"b"`
}

type errOuter struct {
	Inner errInner `avro:"inner"`
}

func TestSemanticError_DecodePath(t *testing.T) {
	schema := avro.MustParse(nestedErrSchema)

	err := avro.Unmarshal(schema, []byte{0x06, 'a'}, &errOuter{})

	var semErr *avro.SemanticError
	require.ErrorAs(t, err, &semErr)
	assert.Equal(t, "Inner.B", semErr.Field)
	assert.Equal(t, "Inner: B: unexpected EOF", err.Error())
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestSemanticError_EncodePath(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"outer","fields":[{"name":"inner","type":{"type":"record","name":"inner","fields":[{"name":"b","type":"int"}]}}]}`)
	type inner struct {
		B any `avro:"b"`
	}
	type outer struct {
		Inner inner `avro:"inner"`
	}

	_, err := avro.Marshal(schema, outer{Inner: inner{B: "x"}})

	var semErr *avro.SemanticError
	require.ErrorAs(t, err, &semErr)
	assert.Equal(t, "Inner.B", semErr.Field)
}

func TestSemanticError_MapRecordPath(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`)

	_, err := avro.Marshal(schema, map[string]any{"a": "x"})

	var semErr *avro.SemanticError
	require.ErrorAs(t, err, &semErr)
	assert.Equal(t, "a", semErr.Field)
}

func TestShortBufferError(t *testing.T) {
	schema := avro.MustParse(nestedErrSchema)

	err := avro.Unmarshal(schema, []byte{0x06, 'a'}, &errOuter{})

	var sbErr *avro.ShortBufferError
	require.ErrorAs(t, err, &sbErr)
	assert.Equal(t, 2, sbErr.Have)
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestShortBufferError_RecordTruncatedInLongOrBetweenFields(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)

	for _, data := range [][]byte{{}, {0x80}, {0x02}, {0x02, 0x80}} {
		err := avro.Unmarshal(schema, data, &TestRecord{})

		var sbErr *avro.ShortBufferError
		require.ErrorAs(t, err, &sbErr, "input % x", data)
		assert.Equal(t, len(data), sbErr.Have)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.NotErrorIs(t, err, io.EOF)
	}
}

func TestShortBufferError_EveryPrefix(t *testing.T) {
	str := "ab"
	tests := []struct {
		name   string
		schema string
		value  any
	}{
		{
			name:   "record",
			schema: `{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`,
			value:  TestRecord{A: 300, B: "foo"},
		},
		{name: "nested record", schema: nestedErrSchema, value: errOuter{Inner: errInner{B: "xy"}}},
		{name: "array", schema: `{"type":"array","items":"long"}`, value: []int64{1, 200}},
		{name: "map", schema: `{"type":"map","values":"int"}`, value: map[string]int{"k": 1}},
		{name: "union", schema: `["null","string"]`, value: &str},
		{name: "double", schema: "double", value: 1.5},
		{name: "fixed", schema: `{"type":"fixed","name":"f","size":3}`, value: [3]byte{1, 2, 3}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(test.schema)
			data, err := avro.Marshal(schema, test.value)
			require.NoError(t, err)

			for n := range data {
				got := reflect.New(reflect.TypeOf(test.value)).Interface()
				err = avro.Unmarshal(schema, data[:n], got)

				var sbErr *avro.ShortBufferError
				require.ErrorAs(t, err, &sbErr, "prefix % x", data[:n])
				assert.Equal(t, n, sbErr.Have)
				assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
				assert.NotErrorIs(t, err, io.EOF)
			}

			got := reflect.New(reflect.TypeOf(test.value))
			require.NoError(t, avro.Unmarshal(schema, data, got.Interface()))
			assert.Equal(t, test.value, got.Elem().Interface())
		})
	}
}

func TestShortBufferError_TrailingBytesIgnored(t *testing.T) {
	schema := avro.MustParse("long")

	var got int64
	err := avro.Unmarshal(schema, []byte{0x36, 0x02}, &got)

	require.NoError(t, err)
	assert.Equal(t, int64(27), got)
}

func TestDecoder_TruncatedStream(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x36, 0x06, 'f', 'o', 'o', 0x02, 0x80}))

	var got TestRecord
	require.NoError(t, dec.Decode(&got))
	err := dec.Decode(&got)

	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	var sbErr *avro.ShortBufferError
	assert.False(t, errors.As(err, &sbErr))
	assert.ErrorIs(t, dec.Decode(&got), io.EOF)
}

func TestShortBufferError_NotForOtherErrors(t *testing.T) {
	schema := avro.MustParse("string")

	var s string
	err := avro.Unmarshal(schema, []byte{0x01}, &s)

	require.Error(t, err)
	var sbErr *avro.ShortBufferError
	assert.False(t, errors.As(err, &sbErr))
}

func TestSemanticError_ManualFieldMessage(t *testing.T) {
	err := &avro.SemanticError{Field: "a.b", Err: io.EOF}

	assert.Equal(t, "a.b: EOF", err.Error())
}

func TestSemanticError_ArrayEncodePathHasSinglePrefix(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"r","fields":[{"name":"l","type":{"type":"array","items":"int"}}]}`)
	type rec struct {
		L []any `avro:"l"`
	}

	_, err := avro.Marshal(schema, rec{L: []any{"x"}})

	var semErr *avro.SemanticError
	require.ErrorAs(t, err, &semErr)
	assert.Equal(t, "L", semErr.Field)
	assert.EqualError(t, err, "L: []interface {}: avro: string is unsupported for Avro int")
}
