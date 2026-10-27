package avro_test

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

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

// retainedPtrEnum is a direct interface type whose text method keeps its receiver.
type retainedPtrEnum struct {
	p *int
}

var retainedPtrEnums []*retainedPtrEnum

func (e *retainedPtrEnum) MarshalText() ([]byte, error) {
	retainedPtrEnums = append(retainedPtrEnums, e)
	return []byte("s" + strconv.Itoa(*e.p)), nil
}

//go:noinline
func clobberStack(n int) int {
	var buf [256]int
	for i := range buf {
		buf[i] = -1
	}
	if n > 0 {
		return clobberStack(n-1) + buf[n]
	}
	return buf[0]
}

func TestEncoder_DirectValueKeepsRetainedReceiverValid(t *testing.T) {
	t.Cleanup(func() { retainedPtrEnums = nil })
	retainedPtrEnums = nil
	schema := avro.MustParse(`{"type":"enum","name":"e","symbols":["s0","s1","s2","s3"]}`)
	v := 2

	_, err := avro.Marshal(schema, retainedPtrEnum{p: &v})
	require.NoError(t, err)
	clobberStack(8)

	require.Len(t, retainedPtrEnums, 1)
	assert.Same(t, &v, retainedPtrEnums[0].p)
}

func TestDecoder_AnyMapDuplicateKeysReplaceValues(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":{"type":"map","values":"long"}}`)
	buf := &bytes.Buffer{}
	w := avro.NewWriter(buf, 64)
	w.WriteBlockHeader(2, 0)
	for _, key := range []string{"x", "y"} {
		w.WriteString("k")
		w.WriteBlockHeader(1, 0)
		w.WriteString(key)
		w.WriteLong(1)
		w.WriteBlockHeader(0, 0)
	}
	w.WriteBlockHeader(0, 0)
	require.NoError(t, w.Flush())

	var got map[string]any
	err := avro.Unmarshal(schema, buf.Bytes(), &got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"k": map[string]any{"y": int64(1)}}, got)
}

// retainedAppender keeps every receiver its text appender sees.
type retainedAppender int

var retainedAppenders []*retainedAppender

func (e *retainedAppender) AppendText(b []byte) ([]byte, error) {
	retainedAppenders = append(retainedAppenders, e)
	return append(b, "s"+strconv.Itoa(int(*e))...), nil
}

func TestEncoder_MapKeepsRetainedAppenderReceiversValid(t *testing.T) {
	t.Cleanup(func() { retainedAppenders = nil })
	retainedAppenders = nil
	schema := avro.MustParse(retainedEnumMapSchema)

	_, err := avro.Marshal(schema, map[string]retainedAppender{"a": 1, "b": 2, "c": 3})
	require.NoError(t, err)

	var got []int
	for _, e := range retainedAppenders {
		got = append(got, int(*e))
	}
	slices.Sort(got)
	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestMap_RoundTripsTimeValues(t *testing.T) {
	schema := avro.MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-micros"}}`)
	in := map[string]time.Time{}
	for i := range 20 {
		in[strconv.Itoa(i)] = time.Date(2020, 1, 1, 0, 0, i, 1000, time.UTC)
	}

	b, err := avro.Marshal(schema, in)
	require.NoError(t, err)
	var got map[string]time.Time
	err = avro.Unmarshal(schema, b, &got)

	require.NoError(t, err)
	assert.Equal(t, in, got)
}
