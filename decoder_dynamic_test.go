package avro_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_Interface(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		schema string
		got    any
		want   any
	}{
		{
			name:   "Empty Interface",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    nil,
			want:   map[string]any{"a": int64(27), "b": "foo"},
		},
		{
			name:   "Interface Non-Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    TestRecord{},
			want:   map[string]any{"a": int64(27), "b": "foo"},
		},
		{
			name:   "Interface Nil Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    (*TestRecord)(nil),
			want:   &TestRecord{A: 27, B: "foo"},
		},
		{
			name:   "Interface Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    &TestRecord{},
			want:   &TestRecord{A: 27, B: "foo"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer ConfigTeardown()

			dec, _ := avro.NewDecoder(test.schema, bytes.NewReader(test.data))

			err := dec.Decode(&test.got)

			require.NoError(t, err)
			assert.Equal(t, test.want, test.got)
		})
	}
}

func TestDecoder_InterfacePrimitiveTypes(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"prims","fields":[
		{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},
		{"name":"f","type":"float"},{"name":"d","type":"double"},{"name":"s","type":"string"},
		{"name":"y","type":"bytes"},{"name":"e","type":"bytes"}]}`)
	want := map[string]any{
		"b": true, "i": 1000, "l": int64(-7), "f": float32(1.5), "d": 2.5,
		"s": "foo", "y": []byte{1, 2}, "e": []byte{},
	}
	data, err := avro.Marshal(schema, want)
	require.NoError(t, err)

	var got any
	err = avro.Unmarshal(schema, data, &got)

	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDecoder_RecordMapFieldsNotAliased(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"outer","fields":[
		{"name":"a1","type":{"type":"array","items":"long"}},
		{"name":"a2","type":{"type":"array","items":"long"}},
		{"name":"r1","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"long"}]}},
		{"name":"r2","type":"inner"},
		{"name":"m1","type":{"type":"map","values":"long"}},
		{"name":"m2","type":{"type":"map","values":"long"}},
		{"name":"d1","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}},
		{"name":"d2","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}]}`)
	data, err := avro.Marshal(schema, map[string]any{
		"a1": []any{int64(1)}, "a2": []any{int64(1)},
		"r1": map[string]any{"x": int64(1)}, "r2": map[string]any{"x": int64(1)},
		"m1": map[string]any{"k": int64(1)}, "m2": map[string]any{"k": int64(1)},
		"d1": big.NewRat(1, 4), "d2": big.NewRat(1, 4),
	})
	require.NoError(t, err)

	var got map[string]any
	err = avro.Unmarshal(schema, data, &got)
	require.NoError(t, err)

	got["a1"].([]any)[0] = int64(2)
	got["r1"].(map[string]any)["x"] = int64(2)
	got["m1"].(map[string]any)["k"] = int64(2)
	got["d1"].(*big.Rat).SetInt64(2)
	assert.Equal(t, []any{int64(1)}, got["a2"])
	assert.Equal(t, map[string]any{"x": int64(1)}, got["r2"])
	assert.Equal(t, map[string]any{"k": int64(1)}, got["m2"])
	assert.Equal(t, big.NewRat(1, 4), got["d2"])
}
