package avro_test

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	mapKey    string
	mapInt16  int16
	mapBool   bool
	mapString string
)

func TestMap_RoundTripsElementLayouts(t *testing.T) {
	api := avro.Config{BlockLength: 2}.Freeze()
	rec := &TestRecord{A: 1, B: "x"}

	tests := []struct {
		name   string
		schema string
		in     any
	}{
		{name: "named key and int16", schema: `{"type":"map","values":"int"}`, in: map[mapKey]mapInt16{"a": 1, "b": -2, "c": 3}},
		{name: "bool", schema: `{"type":"map","values":"boolean"}`, in: map[string]mapBool{"a": true, "b": false, "c": true}},
		{name: "float32", schema: `{"type":"map","values":"float"}`, in: map[string]float32{"a": 1.5, "b": -2}},
		{name: "named string", schema: `{"type":"map","values":"string"}`, in: map[mapKey]mapString{"a": "x", "b": "y", "c": "z"}},
		{name: "int slice", schema: `{"type":"map","values":{"type":"array","items":"int"}}`, in: map[string][]int{"a": {1, 2}, "b": {}, "c": {3}}},
		{name: "pointer", schema: `{"type":"map","values":{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`, in: map[string]*TestRecord{"a": rec, "b": rec, "c": rec}},
		{name: "struct", schema: `{"type":"map","values":{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`, in: map[mapKey]TestRecord{"a": *rec, "b": {A: 2}, "c": {}}},
		{name: "nested map", schema: `{"type":"map","values":{"type":"map","values":"long"}}`, in: map[string]map[string]int64{"a": {"x": 1}, "b": {}, "c": {"y": 2, "z": 3}}},
		{name: "any", schema: `{"type":"map","values":"long"}`, in: map[string]any{"a": int64(1), "b": int64(2), "c": int64(3)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(test.schema)
			out := reflect.New(reflect.TypeOf(test.in))

			b, err := api.Marshal(schema, test.in)
			require.NoError(t, err)
			err = api.Unmarshal(schema, b, out.Interface())

			require.NoError(t, err)
			assert.Equal(t, test.in, out.Elem().Interface())
		})
	}
}

func TestDecoder_MapMergesIntoExistingMap(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":{"type":"record","name":"test","fields":[{"name":"a","type":"long"}]}}`)
	b, err := avro.Marshal(schema, map[string]TestRecord{"x": {A: 1}})
	require.NoError(t, err)

	got := map[string]TestRecord{"x": {A: 0, B: "kept"}, "y": {B: "other"}}
	err = avro.Unmarshal(schema, b, &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]TestRecord{"x": {A: 1, B: "kept"}, "y": {B: "other"}}, got)
}

func TestDecoder_NativeMapMergesIntoExistingMap(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"long"}`)
	b, err := avro.Marshal(schema, map[string]int64{"x": 1})
	require.NoError(t, err)

	got := map[string]int64{"x": 5, "y": 6}
	err = avro.Unmarshal(schema, b, &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"x": 1, "y": 6}, got)
}

func TestMap_RoundTripsPointerTextKeys(t *testing.T) {
	api := avro.Config{BlockLength: 2}.Freeze()
	schema := avro.MustParse(`{"type":"map","values":"string"}`)
	in := map[*benchTextKey]string{}
	for i := range 5 {
		k := benchTextKey(i)
		in[&k] = strconv.Itoa(i)
	}

	b, err := api.Marshal(schema, in)
	require.NoError(t, err)
	var out map[*benchTextKey]string
	err = api.Unmarshal(schema, b, &out)

	require.NoError(t, err)
	got := map[benchTextKey]string{}
	for k, v := range out {
		got[*k] = v
	}
	assert.Equal(t, map[benchTextKey]string{0: "0", 1: "1", 2: "2", 3: "3", 4: "4"}, got)
}

func TestEncoder_MapTimeKeys(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"long"}`)
	key := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	got, err := avro.Marshal(schema, map[time.Time]int64{key: 1})

	require.NoError(t, err)
	want, err := avro.Marshal(schema, map[string]int64{"2020-01-01T00:00:00Z": 1})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
