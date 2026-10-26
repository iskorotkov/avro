package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_InterfaceMixedTypes(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)

	for range 2 {
		got, err := avro.Marshal(schema, []any{1, int64(2), int32(3), 4})
		require.NoError(t, err)
		assert.Equal(t, []byte{0x07, 0x08, 0x02, 0x04, 0x06, 0x08, 0x00}, got)
	}
}

func TestEncoder_UnionMapNilBranchValue(t *testing.T) {
	schema := avro.MustParse(`["null","string"]`)

	_, err := avro.Marshal(schema, map[string]any{"string": nil})

	assert.EqualError(t, err, "avro: nil value for non-null union branch string")
}
