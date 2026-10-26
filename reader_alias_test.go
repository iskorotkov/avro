package avro_test

import (
	"bytes"
	"io"
	"testing"
	"unsafe"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type aliasRecord struct {
	S string `avro:"s"`
	B []byte `avro:"b"`
}

const aliasSchema = `{"type":"record","name":"r","fields":[{"name":"s","type":"string"},{"name":"b","type":"bytes"}]}`

func TestAliasInput_SharesInput(t *testing.T) {
	api := avro.Config{AliasInput: true}.Freeze()
	data := []byte{0x06, 'f', 'o', 'o', 0x04, 0x01, 0x02}

	var got aliasRecord
	err := api.Unmarshal(avro.MustParse(aliasSchema), data, &got)

	require.NoError(t, err)
	assert.Equal(t, "foo", got.S)
	assert.Equal(t, unsafe.Pointer(&data[1]), unsafe.Pointer(unsafe.StringData(got.S)))
	assert.Same(t, &data[5], &got.B[0])
	assert.Equal(t, 2, cap(got.B))
}

func TestAliasInput_DefaultCopies(t *testing.T) {
	data := []byte{0x06, 'f', 'o', 'o', 0x04, 0x01, 0x02}

	var got aliasRecord
	err := avro.Unmarshal(avro.MustParse(aliasSchema), data, &got)

	require.NoError(t, err)
	data[1], data[5] = 'x', 0xff
	assert.Equal(t, aliasRecord{S: "foo", B: []byte{0x01, 0x02}}, got)
}

func TestAliasInput_IgnoredForStreams(t *testing.T) {
	api := avro.Config{AliasInput: true}.Freeze()
	first := []byte{0x06, 'f', 'o', 'o', 0x04, 0x01, 0x02}
	second := []byte{0x06, 'b', 'a', 'r', 0x04, 0x03, 0x04}
	// Each value arrives in its own read, so the second refill overwrites the buffer an alias would point into.
	in := io.MultiReader(bytes.NewReader(first), bytes.NewReader(second))
	dec := api.NewDecoder(avro.MustParse(aliasSchema), in)

	var got1, got2 aliasRecord
	require.NoError(t, dec.Decode(&got1))
	require.NoError(t, dec.Decode(&got2))

	assert.Equal(t, aliasRecord{S: "foo", B: []byte{0x01, 0x02}}, got1)
	assert.Equal(t, aliasRecord{S: "bar", B: []byte{0x03, 0x04}}, got2)
}
