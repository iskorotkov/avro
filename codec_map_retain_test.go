package avro_test

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retainedEnum keeps every receiver its text methods see.
type retainedEnum int

var retainedEnums []*retainedEnum

func (e *retainedEnum) MarshalText() ([]byte, error) {
	retainedEnums = append(retainedEnums, e)
	return []byte("s" + strconv.Itoa(int(*e))), nil
}

func (e *retainedEnum) UnmarshalText(b []byte) error {
	retainedEnums = append(retainedEnums, e)
	i, err := strconv.Atoi(strings.TrimPrefix(string(b), "s"))
	*e = retainedEnum(i)
	return err
}

func retainedValues() []int {
	var got []int
	for _, e := range retainedEnums {
		got = append(got, int(*e))
	}
	slices.Sort(got)
	return got
}

const retainedEnumMapSchema = `{"type":"map","values":{"type":"enum","name":"e","symbols":["s0","s1","s2","s3"]}}`

func TestEncoder_MapKeepsRetainedReceiversValid(t *testing.T) {
	t.Cleanup(func() { retainedEnums = nil })
	retainedEnums = nil
	schema := avro.MustParse(retainedEnumMapSchema)

	_, err := avro.Marshal(schema, map[string]retainedEnum{"a": 1, "b": 2, "c": 3})
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3}, retainedValues())
}

func TestDecoder_MapKeepsRetainedReceiversDistinct(t *testing.T) {
	t.Cleanup(func() { retainedEnums = nil })
	schema := avro.MustParse(retainedEnumMapSchema)
	retainedEnums = nil
	b, err := avro.Marshal(schema, map[string]retainedEnum{"a": 1, "b": 2, "c": 3})
	require.NoError(t, err)
	retainedEnums = nil

	var got map[string]retainedEnum
	err = avro.Unmarshal(schema, b, &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]retainedEnum{"a": 1, "b": 2, "c": 3}, got)
	assert.Equal(t, []int{1, 2, 3}, retainedValues())
}

func TestDecoder_MapTypeConverterKeepsOwnCopies(t *testing.T) {
	var kept []any
	api := avro.Config{}.Freeze()
	api.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, _ avro.Schema) (any, error) {
			kept = append(kept, in)
			return in, nil
		},
	})
	schema := avro.MustParse(`{"type":"map","values":["null","bytes"]}`)
	b, err := api.Marshal(schema, map[string][]byte{"a": []byte("x"), "b": []byte("y")})
	require.NoError(t, err)

	var got map[string][]byte
	err = api.Unmarshal(schema, b, &got)

	require.NoError(t, err)
	var keptStrings []string
	for _, k := range kept {
		keptStrings = append(keptStrings, string(k.([]byte)))
	}
	slices.Sort(keptStrings)
	assert.Equal(t, []string{"x", "y"}, keptStrings)
}

func TestDecoder_MapMergesDuplicateKeys(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":{"type":"map","values":"long"}}`)
	buf := &bytes.Buffer{}
	w := avro.NewWriter(buf, 64)
	w.WriteBlockHeader(2, 0)
	for key, val := range map[string]int64{"x": 1, "y": 2} {
		w.WriteString("k")
		w.WriteBlockHeader(1, 0)
		w.WriteString(key)
		w.WriteLong(val)
		w.WriteBlockHeader(0, 0)
	}
	w.WriteBlockHeader(0, 0)
	require.NoError(t, w.Flush())

	var got map[string]map[string]int64
	err := avro.Unmarshal(schema, buf.Bytes(), &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]int64{"k": {"x": 1, "y": 2}}, got)
}
