package avro_test

import (
	"strings"
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

func TestAppendMarshal_GrowsSmallCapacity(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)
	v := TestRecord{A: 27, B: strings.Repeat("x", 4096)}
	want, err := avro.Marshal(schema, v)
	require.NoError(t, err)
	dst := make([]byte, 1, 2)
	dst[0] = 0xaa

	got, err := avro.AppendMarshal(dst, schema, v)

	require.NoError(t, err)
	assert.Equal(t, append([]byte{0xaa}, want...), got)
	assert.Equal(t, []byte{0xaa}, dst)
}

func TestAppendMarshal_ErrorAfterPartialWrite(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"int"}]}`)
	type rec struct {
		A int64 `avro:"a"`
		B any   `avro:"b"`
	}
	dst := append(make([]byte, 0, 16), 0x01)
	spare := dst[:cap(dst)]

	got, err := avro.AppendMarshal(dst, schema, &rec{A: 27, B: "x"})

	require.Error(t, err)
	assert.Equal(t, []byte{0x01}, got)
	assert.Equal(t, cap(dst), cap(got))
	assert.Same(t, &dst[0], &got[0])
	assert.Equal(t, byte(0x36), spare[1], "the field written before the error is left in spare capacity")
}

func TestAppendMarshal_AppendsToNonEmptyDst(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("string")
	dst := append(make([]byte, 0, 64), 0x01, 0x02, 0x03)

	got, err := avro.AppendMarshal(dst, schema, "foo")
	require.NoError(t, err)
	got, err = avro.AppendMarshal(got, schema, "bar")
	require.NoError(t, err)

	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x06, 'f', 'o', 'o', 0x06, 'b', 'a', 'r'}, got)
	assert.Same(t, &dst[0], &got[0])
}

type marshalOnlyAPI struct{ avro.API }

func TestAppendMarshalWith_FallbackForPlainAPI(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("long")
	api := marshalOnlyAPI{API: avro.DefaultConfig}

	got, err := avro.AppendMarshalWith(api, []byte{0xff}, schema, int64(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xff, 0x36}, got)
}

func TestAppendMarshalWith_FallbackError(t *testing.T) {
	defer ConfigTeardown()

	dst := []byte{0x01}
	api := marshalOnlyAPI{API: avro.DefaultConfig}

	got, err := avro.AppendMarshalWith(api, dst, avro.MustParse("int"), true)

	assert.Error(t, err)
	assert.Equal(t, dst, got)
}

func TestDefaultConfig_ImplementsAppendMarshaler(t *testing.T) {
	_, ok := avro.DefaultConfig.(avro.AppendMarshaler)

	assert.True(t, ok)
}
