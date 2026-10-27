package avro_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pathsRecordSchema = `{"type":"record","name":"r","fields":[{"name":"a","type":"long"},{"name":"b","type":"string","default":"def"}]}`

func TestEncoder_RecordFromNamedKeyMap(t *testing.T) {
	schema := avro.MustParse(pathsRecordSchema)

	got, err := avro.Marshal(schema, map[mapKey]any{"a": int64(3)})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x06, 'd', 'e', 'f'}, got)
}

type pathsLong int64

func (l pathsLong) String() string { return strconv.FormatInt(int64(l), 10) }

func TestEncoder_RecordFromNonEmptyInterfaceMap(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"r","fields":[{"name":"a","type":"long"},{"name":"b","type":"long","default":2}]}`)

	got, err := avro.Marshal(schema, map[mapKey]fmt.Stringer{"a": pathsLong(3)})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x04}, got)
}

func TestEncoder_RecordFromNamedKeyMapMissingField(t *testing.T) {
	schema := avro.MustParse(pathsRecordSchema)

	_, err := avro.Marshal(schema, map[mapKey]any{"b": "x"})

	assert.EqualError(t, err, "avro: missing required field a")
}

func TestDecoder_RecordIntoNamedKeyMap(t *testing.T) {
	schema := avro.MustParse(pathsRecordSchema)

	var got map[mapKey]any
	err := avro.Unmarshal(schema, []byte{0x06, 0x02, 'x'}, &got)

	require.NoError(t, err)
	assert.Equal(t, map[mapKey]any{"a": int64(3), "b": "x"}, got)
}

type pathsRegistered struct{}

func TestConfig_TypeOfAndNamesOf(t *testing.T) {
	api := avro.Config{}.Freeze()
	api.Register("paths.registered", pathsRegistered{})

	typ, err := api.TypeOf("paths.registered")
	require.NoError(t, err)
	names, err := api.NamesOf(reflect.TypeFor[pathsRegistered]())
	require.NoError(t, err)

	assert.Equal(t, reflect.TypeFor[pathsRegistered](), typ)
	assert.Equal(t, []string{"paths.registered"}, names)
}

func TestConfig_TypeOfAndNamesOfUnknown(t *testing.T) {
	api := avro.Config{}.Freeze()

	_, typErr := api.TypeOf("paths.unknown")
	_, nameErr := api.NamesOf(reflect.TypeFor[pathsRegistered]())

	assert.EqualError(t, typErr, "avro: unable to resolve type with name paths.unknown")
	assert.EqualError(t, nameErr, "avro: unable to resolve type avro_test.pathsRegistered")
}

// pathsAppenderKey is a map key whose AppendText fails for negative values.
type pathsAppenderKey int

func (k *pathsAppenderKey) AppendText(b []byte) ([]byte, error) {
	if *k < 0 {
		return nil, errors.New("negative key")
	}
	return append(b, 'k'), nil
}

func TestEncoder_MapTextAppenderKeyErrors(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":"long"}`)
	negative := pathsAppenderKey(-1)

	_, nilErr := avro.Marshal(schema, map[*pathsAppenderKey]int64{nil: 1})
	_, appendErr := avro.Marshal(schema, map[*pathsAppenderKey]int64{&negative: 1})

	assert.EqualError(t, nilErr, "map[*avro_test.pathsAppenderKey]int64: avro: mapEncoderAppender: encoding nil TextAppender")
	assert.EqualError(t, appendErr, "map[*avro_test.pathsAppenderKey]int64: negative key")
}

// pathsUnionConverter holds a union value and the error its conversion to any returns.
type pathsUnionConverter struct {
	val any
	err error
}

func (c *pathsUnionConverter) FromAny(payload any) error {
	if payload == "bad" {
		return errors.New("bad payload")
	}
	c.val = payload
	return nil
}

func (c *pathsUnionConverter) ToAny() (any, error) {
	return c.val, c.err
}

func TestEncoder_UnionConverterErrors(t *testing.T) {
	str := "x"
	tests := []struct {
		name   string
		schema string
		in     *pathsUnionConverter
		want   string
	}{
		{name: "nil not nullable", schema: `["string","long"]`, in: nil, want: "avro: unionConverterToAnyCodec: encoding nil value for non nillable union"},
		{name: "convert error", schema: `["null","string"]`, in: &pathsUnionConverter{err: errors.New("boom")}, want: "avro: unable to convert union: boom"},
		{name: "not a pointer", schema: `["null","string"]`, in: &pathsUnionConverter{val: str}, want: `avro: expected ptr but received "string"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := avro.Marshal(avro.MustParse(test.schema), test.in)

			assert.EqualError(t, err, test.want)
		})
	}
}

func TestEncoder_UnionConverter(t *testing.T) {
	str := "x"
	schema := avro.MustParse(`["null","string"]`)

	got, err := avro.Marshal(schema, &pathsUnionConverter{val: &str})
	require.NoError(t, err)
	gotNil, err := avro.Marshal(schema, (*pathsUnionConverter)(nil))
	require.NoError(t, err)

	assert.Equal(t, []byte{0x02, 0x02, 'x'}, got)
	assert.Equal(t, []byte{0x00}, gotNil)
}

func TestDecoder_NullableUnionTypeConverters(t *testing.T) {
	otherType := avro.TypeConversionFuncs{
		AvroType:              avro.Int,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) { return in, nil },
	}
	wrongType := avro.TypeConversionFuncs{
		AvroType:              avro.Union,
		DecoderTypeConversion: func(any, avro.Schema) (any, error) { return "wrong", nil },
	}
	data := []byte{0x02, 0x02, 'x'}

	t.Run("pointer without union converter", func(t *testing.T) {
		api := avro.Config{}.Freeze()
		api.RegisterTypeConverters(otherType)

		var got *string
		err := api.Unmarshal(avro.MustParse(`["null","string"]`), data, &got)

		require.NoError(t, err)
		assert.Equal(t, "x", *got)
	})
	t.Run("slice without union converter", func(t *testing.T) {
		api := avro.Config{}.Freeze()
		api.RegisterTypeConverters(otherType)

		var got []byte
		err := api.Unmarshal(avro.MustParse(`["null","bytes"]`), data, &got)

		require.NoError(t, err)
		assert.Equal(t, []byte("x"), got)
	})
	t.Run("slice converted to another type", func(t *testing.T) {
		api := avro.Config{}.Freeze()
		api.RegisterTypeConverters(wrongType)

		var got []byte
		err := api.Unmarshal(avro.MustParse(`["null","bytes"]`), data, &got)

		assert.EqualError(t, err, "avro: type converter returned string, expected []uint8")
	})
}

func TestDecoder_NativeMapValueKinds(t *testing.T) {
	tests := []struct {
		schema string
		in     any
	}{
		{schema: `"bytes"`, in: map[string][]byte{"a": []byte("x")}},
		{schema: `"int"`, in: map[string]int32{"a": -1}},
		{schema: `"float"`, in: map[string]float32{"a": 1.5}},
		{schema: `"double"`, in: map[string]float64{"a": -2.5}},
		{schema: `"boolean"`, in: map[string]bool{"a": true}},
	}

	for _, test := range tests {
		t.Run(reflect.TypeOf(test.in).String(), func(t *testing.T) {
			assertMapRoundTrip(t, avro.DefaultConfig, `{"type":"map","values":`+test.schema+`}`, test.in)
		})
	}
}

func TestDecoder_MapErrors(t *testing.T) {
	recordMap := `{"type":"map","values":{"type":"record","name":"r","fields":[{"name":"a","type":"long"}]}}`
	tests := []struct {
		name   string
		api    avro.API
		schema string
		data   []byte
		target any
		want   string
	}{
		{
			name:   "block over limit",
			api:    avro.Config{MaxMapAllocSize: 1}.Freeze(),
			schema: recordMap,
			data:   []byte{0x04, 0x02, 'a', 0x02, 0x02, 'b', 0x04, 0x00},
			target: &map[mapKey]TestRecord{},
			want:   "avro: decode map: size is greater than `Config.MaxMapAllocSize`",
		},
		{
			name:   "element error",
			api:    avro.DefaultConfig,
			schema: `{"type":"map","values":{"type":"enum","name":"e","symbols":["a"]}}`,
			data:   []byte{0x02, 0x02, 'k', 0x0a},
			target: &map[mapKey]mapString{},
			want:   "reading map[avro_test.mapKey]avro_test.mapString: avro: decode enum symbol: unknown enum symbol",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.api.Unmarshal(avro.MustParse(test.schema), test.data, test.target)

			assert.EqualError(t, err, test.want)
		})
	}
}

func TestDecoder_EmptyTextUnmarshalerKeyMap(t *testing.T) {
	var got map[*benchTextKey]string

	err := avro.Unmarshal(avro.MustParse(`{"type":"map","values":"string"}`), []byte{0x00}, &got)

	require.NoError(t, err)
	assert.Equal(t, map[*benchTextKey]string{}, got)
}

func TestEncoder_MapElementError(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":{"type":"enum","name":"e","symbols":["a"]}}`)
	w := avro.NewEncoderForSchema(schema, &bytes.Buffer{})

	err := w.Encode(map[mapKey]string{"k": "unknown"})

	assert.EqualError(t, err, "map[avro_test.mapKey]string: avro: unknown enum symbol: unknown")
}

func TestDecoder_UnionConverter(t *testing.T) {
	schema := avro.MustParse(`["null","string"]`)

	var got *pathsUnionConverter
	err := avro.Unmarshal(schema, []byte{0x02, 0x02, 'x'}, &got)
	require.NoError(t, err)
	var bad *pathsUnionConverter
	badErr := avro.Unmarshal(schema, []byte{0x02, 0x06, 'b', 'a', 'd'}, &bad)

	assert.Equal(t, &pathsUnionConverter{val: "x"}, got)
	assert.EqualError(t, badErr, "avro: Union: bad payload")
}
