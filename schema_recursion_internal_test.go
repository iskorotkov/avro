package avro

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsRecursive(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   bool
	}{
		{name: "flat", schema: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, want: false},
		{
			name:   "nested non-recursive",
			schema: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"S","fields":[]}}},{"name":"b","type":"S"}]}`,
			want:   false,
		},
		{name: "through union", schema: `{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]}]}`, want: true},
		{name: "through array", schema: `{"type":"record","name":"T","fields":[{"name":"c","type":{"type":"array","items":"T"}}]}`, want: true},
		{name: "through map", schema: `{"type":"record","name":"T","fields":[{"name":"c","type":{"type":"map","values":"T"}}]}`, want: true},
		{
			name:   "mutual",
			schema: `{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"]}]}}]}`,
			want:   true,
		},
		{
			name:   "contains recursive record",
			schema: `{"type":"record","name":"R","fields":[{"name":"n","type":{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]}]}}]}`,
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRecursive(MustParse(tt.schema).(*RecordSchema)))
		})
	}
}

func TestIsRecursive_DAGIsLinear(t *testing.T) {
	schema := MustParse(dagSchema(64)).(*RecordSchema)

	start := time.Now()
	got := isRecursive(schema)

	assert.False(t, got)
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}
