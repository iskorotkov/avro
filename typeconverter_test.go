package avro_test

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	boolConverter = avro.TypeConversionFuncs{
		AvroType: avro.Boolean,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			b := in.(bool)
			if b {
				return "yes", nil
			} else {
				return "no", nil
			}
		},
	}

	floatToAvroInt = func(in any, _ avro.Schema) (any, error) {
		switch v := in.(type) {
		case float32:
			if float32(int(v)) != v {
				return 0, fmt.Errorf("%v is not an integer", in)
			}
			return int(v), nil
		case float64:
			if float64(int(v)) != v {
				return 0, fmt.Errorf("%v is not an integer", in)
			}
			return int(v), nil
		case *float64:
			if float64(int(*v)) != *v {
				return 0, fmt.Errorf("%v is not an integer", *v)
			}
			return int(*v), nil
		}
		return in, nil
	}

	intConverter = avro.TypeConversionFuncs{
		AvroType:              avro.Int,
		EncoderTypeConversion: floatToAvroInt,
	}

	fixedDecimalConverter = avro.TypeConversionFuncs{
		AvroType:        avro.Fixed,
		AvroLogicalType: avro.Decimal,
		EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			switch v := in.(type) {
			case string:
				val, _ := new(big.Rat).SetString(v)
				return val, nil
			}
			return in, nil
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			r := in.(*big.Rat)
			f, _ := r.Float64()
			return f, nil
		},
	}

	unionConverter = avro.TypeConversionFuncs{
		AvroType: avro.Union,
		EncoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			union := schema.(*avro.UnionSchema)
			if union.Contains(avro.String) {
				return fmt.Sprintf("%v", in), nil
			}
			if union.Contains(avro.Int) {
				return floatToAvroInt(in, schema)
			}
			return in, nil
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			switch v := in.(type) {
			case bool:
				if v {
					return "yes", nil
				} else {
					return "no", nil
				}
			case *big.Rat:
				f, _ := v.Float64()
				return f, nil
			}

			return in, nil
		},
	}
)

func nonConverter(typ avro.Type) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
	}
}

func errorConverter(typ avro.Type, err error) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
		EncoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return nil, err
		},
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return nil, err
		},
	}
}

func TestDecoderTypeConverter_UnionNullableRegisterAfterFirstDecode(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "int"]`
	data := []byte{0x02, 0x36}

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var first *int
	require.NoError(t, dec.Decode(&first))
	require.NotNil(t, first)
	assert.Equal(t, 27, *first)

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			return in.(int) * 2, nil
		},
	})

	dec2, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var second *int
	require.NoError(t, dec2.Decode(&second))
	require.NotNil(t, second)
	assert.Equal(t, 54, *second, "converter registered after first decode must apply")
}
