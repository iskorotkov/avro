package avro_test

import (
	"sync"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namedInt int

func TestEncoder_InterfaceMixedTypes(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)

	for range 2 {
		got, err := avro.Marshal(schema, []any{1, int64(2), int32(3), 4})
		require.NoError(t, err)
		assert.Equal(t, []byte{0x07, 0x08, 0x02, 0x04, 0x06, 0x08, 0x00}, got)
	}
}

func TestEncoder_InterfaceMixedTypesConcurrent(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	api := avro.Config{}.Freeze()
	want := []byte{0x05, 0x06, 0x02, 0x04, 0x06, 0x00}
	inputs := [][]any{
		{1, int64(2), namedInt(3)},
		{int64(1), namedInt(2), 3},
		{namedInt(1), 2, int64(3)},
	}

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			got, err := api.Marshal(schema, inputs[i%len(inputs)])
			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
	wg.Wait()
}

func TestEncoder_UnionMapNilBranchValue(t *testing.T) {
	schema := avro.MustParse(`["null","string"]`)

	_, err := avro.Marshal(schema, map[string]any{"string": nil})

	assert.EqualError(t, err, "avro: nil value for non-null union branch string")
}

func TestEncoder_InterfaceEncodesAfterMixedTypeError(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	api := avro.Config{}.Freeze()

	_, err := api.Marshal(schema, []any{int64(1)})
	require.NoError(t, err)

	_, err = api.Marshal(schema, []any{int64(1), "x"})
	require.EqualError(t, err, "[]interface {}: []interface {}: avro: string is unsupported for Avro long")

	got, err := api.Marshal(schema, []any{int64(2), int64(3)})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x04, 0x06, 0x00}, got)
}

func TestEncoder_InterfaceFirstTypeErrorDoesNotBlockOthers(t *testing.T) {
	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	api := avro.Config{}.Freeze()

	_, err := api.Marshal(schema, []any{"x"})
	require.EqualError(t, err, "[]interface {}: []interface {}: avro: string is unsupported for Avro long")

	got, err := api.Marshal(schema, []any{int64(2)})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02, 0x04, 0x00}, got)

	_, err = api.Marshal(schema, []any{"y"})
	assert.EqualError(t, err, "[]interface {}: []interface {}: avro: string is unsupported for Avro long")
}
