package avro_test

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingReader wraps an io.Reader and counts all method calls
type countingReader struct {
	reader io.Reader
	count  atomic.Int64
}

func newCountingReader(data []byte) *countingReader {
	return &countingReader{
		reader: bytes.NewReader(data),
	}
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	cr.count.Add(1)
	return cr.reader.Read(p)
}

func (cr *countingReader) getCallCount() int64 {
	return cr.count.Load()
}

const decodeTimeout = 100 * time.Millisecond

func decodeWithTimeout(t *testing.T, decoder *avro.Decoder, v any) error {
	t.Helper()

	done := make(chan error, 1)

	go func() {
		done <- decoder.Decode(v)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(decodeTimeout):
		t.Fatalf("decode did not finish within %s", decodeTimeout)
		return nil
	}
}

func TestDecoder_SkipBool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "boolean"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipInt(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "int"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipLong(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipFloat(t *testing.T) {
	data := []byte{0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "float"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipDouble(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "double"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipBytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "bytes"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipString(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipRecord(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipRef(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x66}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
	    {"name": "b", "type": "string"},
		{"name": "c", "type": "test2"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipRecursiveRecord(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader([]byte{0x00}))
	require.NoError(t, err)

	var got struct {
		Other string `avro:"other"`
	}
	require.NoError(t, decodeWithTimeout(t, dec, &got))
}

func TestDecoder_SkipRecursiveMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record","name":"SchemaData","fields":[{"name":"object","type":{"type":"map","values":"SchemaData"}}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader([]byte{0x00}))
	require.NoError(t, err)

	var got struct {
		Other string `avro:"other"`
	}
	require.NoError(t, decodeWithTimeout(t, dec, &got))
}

func TestDecoder_SkipMutualRecursiveRecord(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":{"type":"map","values":"A"}}]}}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader([]byte{0x00}))
	require.NoError(t, err)

	var got struct {
		Other string `avro:"other"`
	}
	require.NoError(t, decodeWithTimeout(t, dec, &got))
}

func TestDecoder_SkipRecursiveRecordNestedData(t *testing.T) {
	defer ConfigTeardown()

	schemaStr := `{"type":"record","name":"Node","fields":[{"name":"value","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}`

	root := map[string]any{
		"value": "root",
		"children": []any{
			map[string]any{"value": "child1", "children": []any{}},
			map[string]any{
				"value":    "child2",
				"children": []any{map[string]any{"value": "grandchild", "children": []any{}}},
			},
		},
	}

	var buf bytes.Buffer
	enc, err := avro.NewEncoder(schemaStr, &buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(root))

	dec, err := avro.NewDecoder(schemaStr, &buf)
	require.NoError(t, err)
	var got struct {
		Other string `avro:"other"`
	}
	require.NoError(t, decodeWithTimeout(t, dec, &got))
	assert.Empty(t, got.Other)
}

func TestDecoder_SkipRecursiveUnion(t *testing.T) {
	defer ConfigTeardown()

	schemaStr := `{"type":"record","name":"Node","fields":[{"name":"value","type":"string"},{"name":"next","type":["null","Node"]},{"name":"tail","type":"string"}]}`

	root := map[string]any{
		"value": "a",
		"tail":  "end",
		"next": map[string]any{"Node": map[string]any{
			"value": "b",
			"tail":  "inner",
			"next":  map[string]any{"Node": map[string]any{"value": "c", "tail": "x", "next": nil}},
		}},
	}

	var buf bytes.Buffer
	enc, err := avro.NewEncoder(schemaStr, &buf)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(root))

	dec, err := avro.NewDecoder(schemaStr, &buf)
	require.NoError(t, err)
	var got struct {
		Tail string `avro:"tail"`
	}
	require.NoError(t, decodeWithTimeout(t, dec, &got))
	assert.Equal(t, "end", got.Tail)
}

func TestDecoder_SkipEnum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "enum", "name": "test2", "symbols": ["sym1", "sym2"]}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipArray(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipArrayEOF(t *testing.T) {
	defer ConfigTeardown()

	data := avro.EncodeIntToBytes(20999000)
	data = append(data, 0x36, 0x36)
	schema := `{
	"type": "record",
	"name": "test_skip",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
		{"name": "b", "type": "string"}
	]
}`

	reader := newCountingReader(data)
	dec, err := avro.NewDecoder(schema, reader)
	require.NoError(t, err)

	var got TestPartialRecord
	err = decodeWithTimeout(t, dec, &got)

	require.ErrorContains(t, err, "unexpected EOF")
	assert.Less(t, reader.getCallCount(), int64(10), "reader method calls should be much smaller than 20999000")
}

func TestDecoder_SkipArrayBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipMapEOF(t *testing.T) {
	defer ConfigTeardown()

	data := avro.EncodeIntToBytes(20999000)
	data = append(data, 0x02, 0x66)
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
		{"name": "b", "type": "string"}
	]
}`

	reader := newCountingReader(data)
	dec, err := avro.NewDecoder(schema, reader)
	require.NoError(t, err)

	var got TestPartialRecord
	err = decodeWithTimeout(t, dec, &got)

	require.ErrorContains(t, err, "unexpected EOF")
	assert.Less(t, reader.getCallCount(), int64(10), "reader method calls should be much smaller than 20999000")
}

func TestDecoder_SkipMapBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x0C, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnionNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnionInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_SkipFixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "fixed", "name": "test2", "size": 7}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}
