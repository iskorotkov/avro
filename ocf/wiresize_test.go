package ocf

import (
	"fmt"
	"math"
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

func TestMinWireSize_SharedRecordsAreLinear(t *testing.T) {
	schema := `{"type":"record","name":"L0","fields":[{"name":"v","type":"long"}]}`
	for k := 1; k <= 64; k++ {
		schema = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"a","type":%s},{"name":"b","type":"L%d"}]}`, k, schema, k-1)
	}
	w := wireSizer{memo: map[string]int{}}

	got := w.size(avro.MustParse(schema))

	// Each of the 64 levels visits its record, the reference to the previous level and that memoized record; L0 adds its record and long.
	assert.Equal(t, 64*3+2, w.visits)
	assert.Equal(t, math.MaxInt32, got)
}

func TestMinWireSize_SaturatesHugeFixed(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":{"type":"fixed","name":"A","size":2147483647}},
		{"name":"b","type":{"type":"fixed","name":"B","size":2147483647}}]}`)

	assert.Equal(t, math.MaxInt32, minWireSize(schema))
}
