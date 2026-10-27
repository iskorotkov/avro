package avro_test

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_MapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_MapMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]string
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"foo": "foo"}, got)
}

func TestDecoder_MapMapShortRead(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x06}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_MapMapOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"map", "values": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]TestRecord{"foo": {A: 27, B: "foo"}}, got)
}

func TestDecoder_MapOfRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int               `avro:"a"`
		B map[string]record `avro:"b"`
	}

	data := []byte{0x02, 0x01, 0x0c, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x0, 0x0}
	schema := `{
	  "type": "record",
	  "name": "test",
	  "fields": [
		{
		  "name": "a", "type": "int"
		},
		{
		  "name": "b",
		  "type": {
			"type": "map", "values": "test"
		  }
		}
	  ]
	}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got record
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, record{A: 1, B: map[string]record{"foo": {A: 2, B: map[string]record{}}}}, got)
}

func TestDecoder_MapMapError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_MapBigMap(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 20}.Freeze()
	defer ConfigTeardown()

	data := []byte{0x72} // map size 57
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxMapAllocSize`")
}

func TestDecoder_MapMultiBlockExceedsMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	// Two blocks of 3 entries each. Each block alone is under the limit (3 <= 5),
	// but the cumulative count (6) exceeds it — this is the chunking-attack path.
	data := []byte{
		0x06,                   // block 1: l = 3
		0x02, 0x61, 0x02, 0x78, // "a" -> "x"
		0x02, 0x62, 0x02, 0x79, // "b" -> "y"
		0x02, 0x63, 0x02, 0x7a, // "c" -> "z"
		0x06, // block 2: l = 3 (cumulative = 6 > 5)
	}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxMapAllocSize`")
}

func TestDecoder_MapMultiBlockExceedsMaxInt(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,                   // block 1: l = 3
		0x02, 0x61, 0x02, 0x78, // "a" -> "x"
		0x02, 0x62, 0x02, 0x79, // "b" -> "y"
		0x02, 0x63, 0x02, 0x7a, // "c" -> "z"
	}
	// block 2: l = max int - 2
	data = append(data, avro.EncodeIntToBytes(math.MaxInt-2)...)
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxMapAllocSize`")
}

func TestDecoder_MapMultiBlockUnderMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 10}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,                   // block 1: l = 3
		0x02, 0x61, 0x02, 0x78, // "a" -> "x"
		0x02, 0x62, 0x02, 0x79, // "b" -> "y"
		0x02, 0x63, 0x02, 0x7a, // "c" -> "z"
		0x06,                   // block 2: l = 3 (cumulative = 6 <= 10)
		0x02, 0x64, 0x02, 0x77, // "d" -> "w"
		0x02, 0x65, 0x02, 0x76, // "e" -> "v"
		0x02, 0x66, 0x02, 0x75, // "f" -> "u"
		0x00, // end
	}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"a": "x", "b": "y", "c": "z",
		"d": "w", "e": "v", "f": "u",
	}, got)
}

func TestUnmarshal_MapBlockCountExceedsRemainingInput(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"string"}`)
	input := avro.EncodeIntToBytes(1000)

	var out map[string]string
	err := avro.Unmarshal(schema, input, &out)

	require.Error(t, err)
	assert.ErrorIs(t, err, avro.ErrBlockCountExceedsInput)
}

type textUnmarshallerInt int

func (t *textUnmarshallerInt) UnmarshalText(text []byte) error {
	i, err := strconv.Atoi(string(text))
	if err != nil {
		return err
	}
	*t = textUnmarshallerInt(i)
	return nil
}

func TestDecoder_MapUnmarshallerMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x1, 0xe, 0x2, 0x31, 0x8, 0x74, 0x65, 0x73, 0x74, 0x0}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[*textUnmarshallerInt]string
	err := dec.Decode(&got)

	require.NoError(t, err)
	want := map[textUnmarshallerInt]string{1: "test"}
	for k, v := range got {
		wantVal, ok := want[*k]
		assert.True(t, ok)
		assert.Equal(t, wantVal, v)
	}
}

func TestDecoder_MapBigUnmarshallerMap(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 20}.Freeze()
	defer ConfigTeardown()

	data := []byte{0x72} // map size 57
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[*textUnmarshallerInt]string
	err := dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxMapAllocSize`")
}

func TestDecoder_MapMultiBlockUnmarshallerExceedsMaxAlloc(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,                   // block 1: l = 3
		0x02, 0x31, 0x02, 0x78, // "1" -> "x"
		0x02, 0x32, 0x02, 0x79, // "2" -> "y"
		0x02, 0x33, 0x02, 0x7a, // "3" -> "z"
		0x06, // block 2: l = 3 (cumulative = 6 > 5)
	}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[*textUnmarshallerInt]string
	err = dec.Decode(&got)

	assert.ErrorContains(t, err, "size is greater than `Config.MaxMapAllocSize`")
}

func TestDecoder_MapMultiBlockUnmarshallerExceedsMaxInt(t *testing.T) {
	avro.DefaultConfig = avro.Config{MaxMapAllocSize: 5}.Freeze()
	defer ConfigTeardown()

	data := []byte{
		0x06,                   // block 1: l = 3
		0x02, 0x31, 0x02, 0x78, // "1" -> "x"
		0x02, 0x32, 0x02, 0x79, // "2" -> "y"
		0x02, 0x33, 0x02, 0x7a, // "3" -> "z"
	}
	// block 2: l = max int - 2
	data = append(data, avro.EncodeIntToBytes(math.MaxInt-2)...)
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got map[*textUnmarshallerInt]string
	err = dec.Decode(&got)

	require.EqualError(t, err, "avro: decode map: size is greater than `Config.MaxMapAllocSize`")
}

type textUnmarshallerNope int

func (t textUnmarshallerNope) UnmarshalText(text []byte) error {
	i, err := strconv.Atoi(string(text))
	if err != nil {
		return err
	}
	_ = textUnmarshallerNope(i)
	return nil
}

func TestDecoder_MapUnmarshallerMapImpossible(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x1, 0xe, 0x2, 0x31, 0x8, 0x74, 0x65, 0x73, 0x74, 0x0}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[textUnmarshallerNope]string
	err := dec.Decode(&got)

	require.NoError(t, err)
	want := map[textUnmarshallerNope]string{0: "test"}
	assert.Equal(t, want, got)
}

type textUnmarshallerError int

func (t *textUnmarshallerError) UnmarshalText(text []byte) error {
	return errors.New("test")
}

func TestDecoder_MapUnmarshallerKeyError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x1, 0xe, 0x2, 0x31, 0x8, 0x74, 0x65, 0x73, 0x74, 0x0}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[*textUnmarshallerError]string
	err := dec.Decode(&got)

	require.Error(t, err)
}

func TestDecoder_MapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[int]string
	err := dec.Decode(&got)

	assert.Error(t, err)
}
