package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendMarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)
	v := TestRecord{A: 27, B: "foo"}

	want, err := avro.Marshal(schema, v)
	require.NoError(t, err)

	got, err := avro.AppendMarshal([]byte{0xff}, schema, v)

	require.NoError(t, err)
	assert.Equal(t, append([]byte{0xff}, want...), got)
}

func TestAppendMarshal_ReusesBuffer(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("long")
	buf := make([]byte, 0, 16)

	got, err := avro.AppendMarshal(buf, schema, int64(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, got)
	assert.Same(t, &buf[:1][0], &got[0])
}

func TestAppendMarshal_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")
	dst := []byte{0x01}

	got, err := avro.AppendMarshal(dst, schema, true)

	assert.Error(t, err)
	assert.Equal(t, dst, got)
}

func TestAppendMarshal_DoesNotRetainDst(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("string")
	dst := make([]byte, 0, 64)

	got, err := avro.AppendMarshal(dst, schema, "foo")
	require.NoError(t, err)
	_, err = avro.Marshal(schema, "bar")
	require.NoError(t, err)

	assert.Equal(t, []byte{0x06, 'f', 'o', 'o'}, got)
}
