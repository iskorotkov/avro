package avro_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoderTypeConverter_Single(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `{"type":"boolean"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(boolConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "yes", got)
}

func TestDecoderTypeConverter_UnionResolved(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x01}
	schema := `{"type":["null","boolean"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "yes", got)
}

func TestDecoderTypeConverter_MapUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":["null",{"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":4, "scale":2}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	var got map[string]any

	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"fixed_decimal": 346.8}, got)
}

func TestDecoderTypeConverter_UnionNullableSlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 'f', 'o', 'o'}
	schema := `["null", "bytes"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			s := in.([]byte)
			for i, b := range s {
				s[i] = b - 0x20
			}
			return in, nil
		},
	})

	var got []byte
	err := dec.Decode(&got)

	want := []byte("FOO")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDecoderTypeConverter_UnionNullablePtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			i := in.(int)
			i *= 2
			return i, nil
		},
	})

	var got *int
	err := dec.Decode(&got)

	want := int(54)
	require.NoError(t, err)
	assert.Equal(t, want, *got)
}

func TestDecoderTypeConverter_FixedRat(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":4, "scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, float64(346.8), got)
}

func TestDecoderTypeConverter_NotSet(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `{"type":"boolean"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(nonConverter(avro.Boolean))

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, true, got)
}

func TestDecoderTypeConverter_Error(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := `{"type":"int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Int, testError))

	var got any
	err = dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionResolved(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x01}
	schema := `{"type":["null","boolean"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got any
	err = dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionNullableSlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 'f', 'o', 'o'}
	schema := `["null", "bytes"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got []byte
	err := dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionNullablePtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got *int
	err := dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_UnionNullablePtrOwnsConvertedValue(t *testing.T) {
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) {
			return int64(5), nil
		},
	})

	var got *int64
	err := api.Unmarshal(avro.MustParse(`["null", "long"]`), []byte{0x02, 0x02}, &got)
	require.NoError(t, err)
	require.NotNil(t, got)

	*got = 99
	var five any = int64(5)
	assert.Equal(t, int64(5), five)
}

func TestDecoderTypeConverter_UnionNullableStruct(t *testing.T) {
	schema := avro.MustParse(`["null", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`)
	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}

	tests := []struct {
		name    string
		result  any
		want    TestRecord
		wantErr string
	}{
		{name: "value", result: TestRecord{A: 1, B: "converted"}, want: TestRecord{A: 1, B: "converted"}},
		{name: "pointer", result: &TestRecord{A: 1}, want: TestRecord{A: 27, B: "foo"}, wantErr: "avro: type converter returned *avro_test.TestRecord, expected avro_test.TestRecord"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := avro.Config{}.Freeze()
			api.RegisterTypeConverters(avro.TypeConversionFuncs{
				AvroType: avro.Union,
				DecoderTypeConversion: func(any, avro.Schema) (any, error) {
					return test.result, nil
				},
			})

			var got TestRecord
			err := api.Unmarshal(schema, data, &got)

			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.wantErr)
			}
			assert.Equal(t, test.want, got)
		})
	}
}

func TestDecoderTypeConverter_GenericPrimitives(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"prims","fields":[
		{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},
		{"name":"f","type":"float"},{"name":"d","type":"double"},{"name":"s","type":"string"},
		{"name":"y","type":"bytes"}]}`)
	data, err := avro.Marshal(schema, map[string]any{
		"b": true, "i": 1, "l": int64(2), "f": float32(3), "d": 4.0, "s": "foo", "y": []byte{5},
	})
	require.NoError(t, err)

	api := avro.Config{}.Freeze()
	for _, typ := range []avro.Type{avro.Boolean, avro.Int, avro.Long, avro.Float, avro.Double, avro.String, avro.Bytes} {
		api.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType: typ,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return fmt.Sprintf("%s:%v", typ, in), nil
			},
		})
	}

	var got map[string]any
	err = api.Unmarshal(schema, data, &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"b": "boolean:true", "i": "int:1", "l": "long:2", "f": "float:3", "d": "double:4", "s": "string:foo", "y": "bytes:[5]",
	}, got)
}

func TestDecoderTypeConverter_GenericPrimitiveError(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"prims","fields":[{"name":"l","type":"long"}]}`)
	errConv := errors.New("conversion failed")
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Long,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) {
			return nil, errConv
		},
	})

	var got map[string]any
	err := api.Unmarshal(schema, []byte{0x02}, &got)

	assert.ErrorIs(t, err, errConv)
}

func TestDecoderTypeConverter_MapUnionBranch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   []byte
		want   map[string]any
	}{
		{
			name:   "long",
			schema: `["null","long"]`,
			data:   []byte{0x02, 0x36},
			want:   map[string]any{"long": "27"},
		},
		{
			name:   "timestamp-millis",
			schema: `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			data:   []byte{0x02, 0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b},
			want:   map[string]any{"long.timestamp-millis": "2020-01-02T03:04:05Z"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer ConfigTeardown()

			avro.RegisterTypeConverters(longStringConverter, timestampMillisConverter)

			var got map[string]any
			err := avro.Unmarshal(avro.MustParse(test.schema), test.data, &got)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestDecoderTypeConverter_UnionNullablePtrResult(t *testing.T) {
	tests := []struct {
		name    string
		convert func(in any) any
		want    *int
		wantErr string
	}{
		{
			name:    "pointer",
			convert: func(in any) any { v := in.(int) * 2; return &v },
			want:    new(54),
		},
		{
			name:    "element value",
			convert: func(in any) any { return in.(int) * 2 },
			want:    new(54),
		},
		{
			name:    "wrong pointer type",
			convert: func(in any) any { v := int64(in.(int)); return &v },
			wantErr: "avro: type converter returned *int64, expected *int",
		},
		{
			name:    "wrong value type",
			convert: func(in any) any { return "27" },
			wantErr: "avro: type converter returned string, expected *int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer ConfigTeardown()

			avro.RegisterTypeConverters(avro.TypeConversionFuncs{
				AvroType: avro.Union,
				DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
					return test.convert(in), nil
				},
			})

			var got *int
			err := avro.Unmarshal(avro.MustParse(`["null", "int"]`), []byte{0x02, 0x36}, &got)

			if test.wantErr != "" {
				assert.EqualError(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestDecoderTypeConverter_UnionNullableSliceWrongType(t *testing.T) {
	defer ConfigTeardown()

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return string(in.([]byte)), nil
		},
	})

	var got []byte
	err := avro.Unmarshal(avro.MustParse(`["null", "bytes"]`), []byte{0x02, 0x06, 'f', 'o', 'o'}, &got)

	assert.EqualError(t, err, "avro: type converter returned string, expected []uint8")
}

type (
	namedIntPtr *int
	namedBytes  []byte
)

func TestDecoderTypeConverter_UnionNullableAssignable(t *testing.T) {
	t.Run("named pointer", func(t *testing.T) {
		defer ConfigTeardown()
		avro.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType: avro.Union,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				v := in.(int) * 2
				return &v, nil
			},
		})

		var got namedIntPtr
		err := avro.Unmarshal(avro.MustParse(`["null", "int"]`), []byte{0x02, 0x36}, &got)

		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, 54, *got)
	})

	t.Run("named pointer element value", func(t *testing.T) {
		defer ConfigTeardown()
		avro.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType: avro.Union,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return in.(int) * 2, nil
			},
		})

		var got namedIntPtr
		err := avro.Unmarshal(avro.MustParse(`["null", "int"]`), []byte{0x02, 0x36}, &got)

		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, 54, *got)
	})

	t.Run("named slice", func(t *testing.T) {
		defer ConfigTeardown()
		avro.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType: avro.Union,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return namedBytes(bytes.ToUpper(in.([]byte))), nil
			},
		})

		var got []byte
		err := avro.Unmarshal(avro.MustParse(`["null", "bytes"]`), []byte{0x02, 0x06, 'f', 'o', 'o'}, &got)

		require.NoError(t, err)
		assert.Equal(t, []byte("FOO"), got)
	})

	t.Run("pointer to any", func(t *testing.T) {
		defer ConfigTeardown()
		avro.RegisterTypeConverters(avro.TypeConversionFuncs{
			AvroType: avro.Union,
			DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
				return "converted", nil
			},
		})

		var got *any
		err := avro.Unmarshal(avro.MustParse(`["null", "int"]`), []byte{0x02, 0x36}, &got)

		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "converted", *got)
	})
}

func TestDecoderTypeConverter_UnionNullableWithoutUnionConverter(t *testing.T) {
	schema := avro.MustParse(`["null", "int"]`)
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(
		intConverter,
		avro.TypeConversionFuncs{AvroType: avro.Union, EncoderTypeConversion: floatToAvroInt},
	)

	var got *int
	err := api.Unmarshal(schema, []byte{0x02, 0x36}, &got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 27, *got)
}

func TestDecoderTypeConverter_ErrorWinsOverStoreError(t *testing.T) {
	schema := avro.MustParse(`["null", "int"]`)
	testError := errors.New("test error")
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) {
			return "not an int", testError
		},
	})

	var got *int
	err := api.Unmarshal(schema, []byte{0x02, 0x36}, &got)

	assert.ErrorIs(t, err, testError)
	assert.NotContains(t, err.Error(), "type converter returned")
}

func TestDecoderTypeConverter_UnionIntoInterfaceUsesUnionConverterOnly(t *testing.T) {
	schema := avro.MustParse(`["null", "long"]`)
	data := []byte{0x02, 0x36}
	unionConv := avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return fmt.Sprintf("union:%v", in), nil
		},
	}

	tests := []struct {
		name  string
		convs []avro.TypeConverter
		want  any
	}{
		{name: "branch only", convs: []avro.TypeConverter{longStringConverter}, want: int64(27)},
		{name: "branch and union", convs: []avro.TypeConverter{longStringConverter, unionConv}, want: "union:27"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := avro.Config{}.Freeze()
			api.RegisterTypeConverters(test.convs...)

			var got any
			err := api.Unmarshal(schema, data, &got)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}
