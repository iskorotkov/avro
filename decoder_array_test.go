package avro_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_ArraySlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []int{27, 28}, got)
}

func TestDecoder_ArraySliceShortRead(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArraySliceOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}}, got)
}

func TestDecoder_ArrayRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int      `avro:"a"`
		B []record `avro:"b"`
	}

	data := []byte{0x2, 0x3, 0x8, 0x4, 0x0, 0x6, 0x0, 0x0}
	schema := `{
	  "type": "record",
	  "name": "test",
	  "fields": [
		{
		  "name": "a",
		  "type": "int"
		},
		{
		  "name": "b",
		  "type": {
			"type": "array",
			"items": "test"
		  }
		}
	  ]
	}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got record
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, record{A: 1, B: []record{{A: 2, B: []record{}}, {A: 3, B: []record{}}}}, got)
}

func TestDecoder_ArraySliceError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArraySliceItemError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayMaxAllocationError(t *testing.T) {
	defer ConfigTeardown()
	data := []byte{0x2, 0x0, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0xe9, 0x0}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayExceedMaxSliceAllocationConfig(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	// 10 (long) gets encoded to 0x14
	data := []byte{0x14}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArrayMultiBlockExceedsMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	// Two blocks of 3 entries each. Each block alone is under the limit (3 <= 5),
	// but the cumulative count (6) exceeds it — this is the chunking-attack path.
	data := []byte{
		0x06,             // block 1: l = 3
		0x00, 0x01, 0x00, // false, true, false
		0x06, // block 2: l = 3 (cumulative = 6 > 5)
	}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxSliceAllocSize`")
}

func TestDecoder_ArraySliceExceedsMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,             // block 1: l = 3
		0x00, 0x01, 0x00, // 3 int values
		0x06, // block 2: l = 3 (cumulative = 6 > 5)
	}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxSliceAllocSize`")
}

func TestDecoder_ArraySliceExceedsMaxInt(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,             // block 1: l = 3
		0x00, 0x01, 0x00, // 3 int values
	}
	// block 2: l = max int - 2
	data = append(data, avro.EncodeIntToBytes(math.MaxInt-2)...)

	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxSliceAllocSize`")
}

func TestDecoder_ArrayMultiBlockExceedsMaxInt(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 13}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,             // block 1: l = 3
		0x00, 0x01, 0x00, // false, true, false
	}
	// block 2: l = max int - 2
	data = append(data, avro.EncodeIntToBytes(math.MaxInt-2)...)

	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxSliceAllocSize`")
}

func TestDecoder_ArrayMultiBlockUnderMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 10}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,             // block 1: l = 3
		0x00, 0x01, 0x00, // false, true, false
		0x06,             // block 2: l = 3 (cumulative = 6 <= 10)
		0x01, 0x00, 0x01, // true, false, true
		0x00, // end
	}
	schema := `{"type":"array", "items": { "type": "boolean" }}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []bool
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []bool{false, true, false, true, false, true}, got)
}

func TestUnmarshal_HugeArrayLengthDoesNotPanic(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxSliceAllocSize: 1 << 22}.Freeze()
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	input := avro.EncodeIntToBytes(math.MaxInt - 2)

	var out []int64
	err := avro.Unmarshal(schema, input, &out)

	require.Error(t, err)
	assert.ErrorContains(t, err, "Config.MaxSliceAllocSize")
}

func TestUnmarshal_HugeBytesLengthDoesNotPanic(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxByteSliceSize: 1 << 20}.Freeze()
	defer ConfigTeardown()

	schema := avro.MustParse(`"bytes"`)
	input := avro.EncodeIntToBytes(math.MaxInt - 2)

	var out []byte
	err := avro.Unmarshal(schema, input, &out)

	require.Error(t, err)
	assert.ErrorContains(t, err, "Config.MaxByteSliceSize")
}

func TestUnmarshal_HugeMapBlockDoesNotPanic(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 1 << 20}.Freeze()
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"string"}`)
	input := avro.EncodeIntToBytes(math.MaxInt - 2)

	var out map[string]string
	err := avro.Unmarshal(schema, input, &out)

	require.Error(t, err)
	assert.ErrorContains(t, err, "Config.MaxMapAllocSize")
}
