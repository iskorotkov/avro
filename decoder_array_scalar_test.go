package avro_test

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func roundTripArray(t *testing.T, schemaStr string, src, dst any) {
	t.Helper()

	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schemaStr, buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(src))

	dec, err := avro.NewDecoder(schemaStr, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.NoError(t, dec.Decode(dst))
}

func roundTripScalarArray[T any](t *testing.T, schemaStr string, want []T) {
	t.Helper()

	var got []T
	roundTripArray(t, schemaStr, want, &got)
	assert.Equal(t, want, got)
}

func TestDecoder_ArrayScalarRoundTrip(t *testing.T) {
	defer ConfigTeardown()

	t.Run("int32", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []int32{0, -1, 1, 2147483647, -2147483648, 42})
	})
	t.Run("int32 empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []int32{})
	})
	t.Run("int64", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"long"}`, []int64{0, -1, 1, 9223372036854775807, -9223372036854775808, 42})
	})
	t.Run("int64 empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"long"}`, []int64{})
	})
	t.Run("float32", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"float"}`, []float32{0, -1.5, 3.14, 1e10})
	})
	t.Run("float32 empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"float"}`, []float32{})
	})
	t.Run("float64", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"double"}`, []float64{0, -1.5, 3.141592653589793, 1e100})
	})
	t.Run("float64 empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"double"}`, []float64{})
	})
	t.Run("bool", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"boolean"}`, []bool{true, false, true, true, false})
	})
	t.Run("bool empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"boolean"}`, []bool{})
	})
	t.Run("string", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"string"}`, []string{"", "a", "foo", "пример", "🚀"})
	})
	t.Run("string empty", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"string"}`, []string{})
	})
}

func TestDecoder_ArrayScalarNilSlice(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array","items":"long"}`

	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode([]int64(nil)))

	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var got []int64
	require.NoError(t, dec.Decode(&got))
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func roundTripMultiBlockArray[T any](t *testing.T, schemaStr string, want []T) {
	t.Helper()

	cfg := avro.Config{BlockLength: 1}.Freeze()
	parsed, err := avro.Parse(schemaStr)
	require.NoError(t, err)

	buf := &bytes.Buffer{}
	enc := cfg.NewEncoder(parsed, buf)
	require.NoError(t, enc.Encode(want))

	dec, err := avro.NewDecoder(schemaStr, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var got []T
	require.NoError(t, dec.Decode(&got))
	assert.Equal(t, want, got)
}

func TestDecoder_ArrayScalarMultiBlock(t *testing.T) {
	defer ConfigTeardown()

	t.Run("int", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"int"}`, []int32{0, -1, 1, 2147483647, -2147483648, 42, 100})
	})
	t.Run("long", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"long"}`, []int64{0, -1, 1, 9223372036854775807, -9223372036854775808, 42})
	})
	t.Run("bool", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"boolean"}`, []bool{true, false, true, true, false})
	})
	t.Run("float", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"float"}`, []float32{0, -1.5, 3.14, 1e10})
	})
	t.Run("double", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"double"}`, []float64{1.1, 2.2, 3.3, 4.4, 5.5})
	})
	t.Run("string", func(t *testing.T) {
		roundTripMultiBlockArray(t, `{"type":"array","items":"string"}`, []string{"a", "bb", "ccc", "dddd"})
	})
}

func TestEncoder_ArrayScalarIntKinds(t *testing.T) {
	defer ConfigTeardown()

	t.Run("int8", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []int8{0, -1, 1, 127, -128, 42})
	})
	t.Run("int16", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []int16{0, -1, 1, 32767, -32768, 4242})
	})
	t.Run("uint", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []uint{0, 1, 42, 1234567})
	})
	t.Run("uint8", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []uint8{0, 1, 127, 128, 255})
	})
	t.Run("uint16", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []uint16{0, 1, 32767, 32768, 65535})
	})
	t.Run("long/int32", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"long"}`, []int32{0, -1, 1, 2147483647, -2147483648})
	})
	t.Run("long/uint32", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"long"}`, []uint32{0, 1, 2147483647, 2147483648, 4294967295})
	})
}

func TestDecoder_ArrayScalarNamedTypeAlias(t *testing.T) {
	defer ConfigTeardown()

	type ID int32

	roundTripScalarArray(t, `{"type":"array","items":"int"}`, []ID{1, 2, 3, 4, 5})
}

func TestDecoder_ArrayLogicalTypeRoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`
	want := []time.Time{
		time.UnixMilli(0).UTC(),
		time.UnixMilli(1700000000000).UTC(),
	}

	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(want))

	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var got []time.Time
	require.NoError(t, dec.Decode(&got))
	require.Len(t, got, len(want))

	for i := range want {
		assert.True(t, want[i].Equal(got[i]), "i=%d want=%v got=%v", i, want[i], got[i])
	}
}

func TestDecoder_ArrayNullableUnionRoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array","items":["null","long"]}`
	v1 := int64(7)
	v2 := int64(42)
	want := []*int64{&v1, nil, &v2}

	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(want))

	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	var got []*int64
	require.NoError(t, dec.Decode(&got))
	require.Len(t, got, len(want))
	assert.Equal(t, *want[0], *got[0])
	assert.Nil(t, got[1])
	assert.Equal(t, *want[2], *got[2])
}

func TestDecoder_ArrayScalarSchemaResolution(t *testing.T) {
	defer ConfigTeardown()

	writerSchemaStr := `{"type":"array","items":"int"}`
	src := []int32{1, 2, 3, 4, 5}

	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(writerSchemaStr, buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(src))

	writer := avro.MustParse(writerSchemaStr)
	reader := avro.MustParse(`{"type":"array","items":"long"}`)

	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	dec := avro.NewDecoderForSchema(resolved, bytes.NewReader(buf.Bytes()))

	var got []int64
	require.NoError(t, dec.Decode(&got))
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, got)
}

func TestDecoder_ArrayScalarGoInt(t *testing.T) {
	defer ConfigTeardown()

	t.Run("int schema", func(t *testing.T) {
		roundTripScalarArray(t, `{"type":"array","items":"int"}`, []int{0, -1, 1, 1234567})
	})
	t.Run("long schema", func(t *testing.T) {
		if strconv.IntSize != 64 {
			t.Skipf("requires 64-bit int (got %d-bit)", strconv.IntSize)
		}
		// `1 << 50` as an untyped constant in []int{...} would overflow int at
		// compile time on 32-bit. Route through a typed int64 so the conversion
		// happens at runtime, gated by the strconv.IntSize check above.
		var v int64 = 1 << 50
		roundTripScalarArray(t, `{"type":"array","items":"long"}`, []int{0, -1, 1, int(v)})
	})
}

func TestDecoder_ArrayScalarRespectsMaxSliceAllocSize(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		decode func(*avro.Decoder) error
	}{
		{"long", `{"type":"array","items":"long"}`, func(d *avro.Decoder) error { var v []int64; return d.Decode(&v) }},
		{"int", `{"type":"array","items":"int"}`, func(d *avro.Decoder) error { var v []int32; return d.Decode(&v) }},
		{"float", `{"type":"array","items":"float"}`, func(d *avro.Decoder) error { var v []float32; return d.Decode(&v) }},
		{"double", `{"type":"array","items":"double"}`, func(d *avro.Decoder) error { var v []float64; return d.Decode(&v) }},
		{"bool", `{"type":"array","items":"boolean"}`, func(d *avro.Decoder) error { var v []bool; return d.Decode(&v) }},
		{"string", `{"type":"array","items":"string"}`, func(d *avro.Decoder) error { var v []string; return d.Decode(&v) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()
			defer ConfigTeardown()

			data := []byte{0x14}
			dec, err := avro.NewDecoder(tc.schema, bytes.NewReader(data))
			require.NoError(t, err)

			err = tc.decode(dec)
			assert.Error(t, err)
		})
	}
}

func TestDecoder_ArrayScalarPropagatesElementError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array","items":"int"}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int32
	err = dec.Decode(&got)
	assert.Error(t, err)
}
