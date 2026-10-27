package avro_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoderTypeConverter_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	val := []any{
		float32(27),
		float64(28),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordSingle(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	val := map[string]any{
		"a": float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordNullable(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["null","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	val := map[string]any{
		"a": float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordNullablePtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["null","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	f := float64(27)
	val := map[string]any{
		"a": &f,
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordUnionResolved(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["string","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	val := map[string]any{
		"a": float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x04, 0x32, 0x37}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordMapUnion(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["null","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	val := map[string]any{
		"a": map[string]any{"int": float64(27)},
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_MapRecordUnionFixedDecimal(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type": {"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":5, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	val := map[string]any{
		"a": "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConverter_StructRecordSingle(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	type TestRecord struct {
		A any `avro:"a"`
	}

	val := TestRecord{
		A: float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_StructRecordNullable(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["null","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	type TestRecord struct {
		A any `avro:"a"`
	}

	val := TestRecord{
		A: float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, buf.Bytes())
}

func TestEncoderTypeConverter_StructRecordUnionResolved(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["string","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	type TestRecord struct {
		A any `avro:"a"`
	}

	val := TestRecord{
		A: float64(27),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x04, 0x32, 0x37}, buf.Bytes())
}

func TestEncoderTypeConverter_StructRecordFixedDecimal(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type": {"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":5, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	type TestRecord struct {
		A any `avro:"a"`
	}

	val := TestRecord{
		A: "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConverter_UnionInterfaceUnregisteredArray(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", {"type":"array", "items":"int"}]`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	var val any = map[string]any{
		"array": []any{
			float32(27),
			float64(28),
		},
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x03, 0x04, 0x36, 0x38, 0x00}, buf.Bytes())
}

func TestEncoderTypeConverter_NotSet(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(nonConverter(avro.Int))

	val := []any{
		27,
		28,
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoderTypeConverter_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Int, testError))

	val := []any{
		float32(27.1),
	}
	err = enc.Encode(val)

	assert.ErrorIs(t, err, testError)
	assert.Empty(t, buf.Bytes())
}

func TestEncoderTypeConverter_ErrorMapUnion(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":["null","int"]}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		EncoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			if _, ok := in.(int); ok {
				return nil, testError
			}
			return in, nil
		},
	})

	val := map[string]any{
		"a": map[string]any{"int": 27},
	}
	err = enc.Encode(val)

	assert.ErrorIs(t, err, testError)
	assert.Empty(t, buf.Bytes())
}

func TestEncoderTypeConverter_MapUnionBranch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		val    map[string]any
		want   []byte
	}{
		{
			name:   "long",
			schema: `["null","long"]`,
			val:    map[string]any{"long": "27"},
			want:   []byte{0x02, 0x36},
		},
		{
			name:   "timestamp-millis",
			schema: `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			val:    map[string]any{"long.timestamp-millis": "2020-01-02T03:04:05Z"},
			want:   []byte{0x02, 0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer ConfigTeardown()

			avro.RegisterTypeConverters(longStringConverter, timestampMillisConverter)

			got, err := avro.Marshal(avro.MustParse(test.schema), test.val)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestEncoderTypeConverter_MapUnionFallsBackToUnion(t *testing.T) {
	defer ConfigTeardown()

	avro.RegisterTypeConverters(
		avro.TypeConversionFuncs{
			AvroType: avro.Long,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return in, nil
			},
		},
		avro.TypeConversionFuncs{
			AvroType: avro.Union,
			EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return strconv.ParseInt(in.(string), 10, 64)
			},
		},
	)

	got, err := avro.Marshal(avro.MustParse(`["null","long"]`), map[string]any{"long": "27"})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, got)
}

func TestEncoderTypeConverter_MapUnionBranchWinsOverUnion(t *testing.T) {
	unionCalls := 0
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(
		longStringConverter,
		avro.TypeConversionFuncs{
			AvroType: avro.Union,
			EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				unionCalls++
				return int64(1), nil
			},
		},
	)

	got, err := api.Marshal(avro.MustParse(`["null","long"]`), map[string]any{"long": "27"})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, got)
	assert.Zero(t, unionCalls)
}

func TestEncoderTypeConverter_MapUnionBranchErrorDoesNotFallBack(t *testing.T) {
	branchError := errors.New("branch error")
	unionCalls := 0
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(
		errorConverter(avro.Long, branchError),
		avro.TypeConversionFuncs{
			AvroType: avro.Union,
			EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				unionCalls++
				return int64(1), nil
			},
		},
	)

	_, err := api.Marshal(avro.MustParse(`["null","long"]`), map[string]any{"long": "27"})

	assert.ErrorIs(t, err, branchError)
	assert.Zero(t, unionCalls)
}
