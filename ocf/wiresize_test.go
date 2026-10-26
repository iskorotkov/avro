package ocf

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestMinWireSize(t *testing.T) {
	tests := []struct {
		schema string
		want   int
	}{
		{schema: `"null"`, want: 0},
		{schema: `"boolean"`, want: 1},
		{schema: `"long"`, want: 1},
		{schema: `"float"`, want: 4},
		{schema: `"double"`, want: 8},
		{schema: `"string"`, want: 1},
		{schema: `{"type":"int","logicalType":"date"}`, want: 1},
		{schema: `{"type":"fixed","name":"F","size":16}`, want: 16},
		{schema: `["null","long"]`, want: 1},
		{schema: `{"type":"array","items":"double"}`, want: 1},
		{schema: `{"type":"record","name":"Empty","fields":[]}`, want: 0},
		{schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"double"},{"name":"b","type":"null"},{"name":"c","type":{"type":"fixed","name":"G","size":3}}]}`, want: 11},
		{schema: `{"type":"record","name":"Loop","fields":[{"name":"x","type":"long"},{"name":"self","type":"Loop"}]}`, want: 1},
		{schema: `{"type":"record","name":"Node","fields":[{"name":"v","type":"float"},{"name":"next","type":["null","Node"]}]}`, want: 5},
	}

	for _, tt := range tests {
		t.Run(tt.schema, func(t *testing.T) {
			assert.Equal(t, tt.want, minWireSize(avro.MustParse(tt.schema)))
		})
	}
}
