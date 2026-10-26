package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinWireSize(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   int
	}{
		{name: "null", schema: `"null"`, want: 0},
		{name: "boolean", schema: `"boolean"`, want: 1},
		{name: "string", schema: `"string"`, want: 1},
		{name: "float", schema: `"float"`, want: 4},
		{name: "double", schema: `"double"`, want: 8},
		{name: "enum", schema: `{"type":"enum","name":"E","symbols":["A"]}`, want: 1},
		{name: "fixed", schema: `{"type":"fixed","name":"F","size":16}`, want: 16},
		{name: "array", schema: `{"type":"array","items":"double"}`, want: 1},
		{name: "map", schema: `{"type":"map","values":"double"}`, want: 1},
		{name: "union", schema: `["double","float"]`, want: 5},
		{name: "nullable union", schema: `["null","double"]`, want: 1},
		{name: "empty record", schema: `{"type":"record","name":"R","fields":[]}`, want: 0},
		{
			name:   "record",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"double"},{"name":"b","type":"null"},{"name":"c","type":"long"}]}`,
			want:   9,
		},
		{
			name:   "recursive record",
			schema: `{"type":"record","name":"N","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","N"]}]}`,
			want:   2,
		},
		{
			name: "repeated reference",
			schema: `{"type":"record","name":"R","fields":[
				{"name":"a","type":{"type":"fixed","name":"F","size":3}},
				{"name":"b","type":"F"}
			]}`,
			want: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, minWireSize(MustParse(tt.schema)))
		})
	}
}

func TestMinWireSize_SaturatesHugeRecords(t *testing.T) {
	schema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":{"type":"fixed","name":"F","size":1073741824}},
		{"name":"b","type":"F"}
	]}`)

	assert.Equal(t, maxWireSize, minWireSize(schema))
}
