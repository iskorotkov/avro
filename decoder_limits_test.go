package avro_test

import (
	"bytes"
	"io"
	"net/netip"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const linkedListSchema = `{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]}]}`

type linkedNode struct {
	Next *linkedNode `avro:"next"`
}

type otherFieldOnly struct {
	Other string `avro:"other"`
}

// linkedListPayload encodes a list of n nodes.
func linkedListPayload(n int) []byte {
	return append(bytes.Repeat([]byte{0x02}, n), 0x00)
}

func TestUnmarshal_DeepRecursionReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	data := linkedListPayload(3_000_000)

	tests := []struct {
		name string
		v    any
	}{
		{name: "struct", v: &linkedNode{}},
		{name: "any", v: new(any)},
		{name: "skip", v: &otherFieldOnly{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := avro.Unmarshal(schema, data, tt.v)

			require.ErrorContains(t, err, "Config.MaxDepth")
		})
	}
}

func TestReader_ReadNextDeepRecursionReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	r := avro.NewReader(bytes.NewReader(linkedListPayload(3_000_000)), 1024)

	_ = r.ReadNext(schema)

	require.ErrorContains(t, r.Error, "Config.MaxDepth")
}

func TestUnmarshal_DepthWithinLimitSucceeds(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)

	var got linkedNode
	err := avro.Unmarshal(schema, linkedListPayload(300), &got)

	require.NoError(t, err)
}

func TestUnmarshal_MaxDepthIsConfigurable(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	api := avro.Config{MaxDepth: 5}.Freeze()

	var got linkedNode
	require.NoError(t, api.Unmarshal(schema, linkedListPayload(4), &got))
	require.ErrorContains(t, api.Unmarshal(schema, linkedListPayload(5), &got), "Config.MaxDepth")
}

func TestMarshal_CyclicValueReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	node := &linkedNode{}
	node.Next = node

	_, err := avro.Marshal(schema, node)

	require.ErrorContains(t, err, "Config.MaxDepth")
}

func TestMarshal_CyclicGenericValueReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"M","fields":[{"name":"m","type":{"type":"map","values":["null","M"]}}]}`)
	inner := map[string]any{}
	value := map[string]any{"m": inner}
	inner["self"] = map[string]any{"M": value}

	_, err := avro.Marshal(schema, value)

	require.ErrorContains(t, err, "Config.MaxDepth")
}

func TestUnmarshal_ForgedBlockCountsReturnError(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name    string
		schema  string
		data    []byte
		v       any
		wantErr string
	}{
		{
			name:    "array over slice limit",
			schema:  `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`,
			data:    avro.EncodeIntToBytes(1_000_000_000),
			v:       new([]struct{ A int64 }),
			wantErr: "Config.MaxSliceAllocSize",
		},
		{
			name:    "array over remaining input",
			schema:  `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new([]struct{ A int64 }),
			wantErr: "block count exceeds remaining input",
		},
		{
			name:    "scalar array over remaining input",
			schema:  `{"type":"array","items":"double"}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new([]float64),
			wantErr: "block count exceeds remaining input",
		},
		{
			name:    "map over remaining input",
			schema:  `{"type":"map","values":"int"}`,
			data:    avro.EncodeIntToBytes(100_000_000),
			v:       new(map[string]int),
			wantErr: "Config.MaxMapAllocSize",
		},
		{
			name:    "map into any over remaining input",
			schema:  `{"type":"map","values":"null"}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new(any),
			wantErr: "block count exceeds remaining input",
		},
		{
			name:    "zero width items",
			schema:  `{"type":"array","items":"null"}`,
			data:    avro.EncodeIntToBytes(2_000_000),
			v:       new([]any),
			wantErr: "too many items without wire representation",
		},
		{
			name:    "zero width items into any",
			schema:  `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
			data:    avro.EncodeIntToBytes(2_000_000),
			v:       new(any),
			wantErr: "too many items without wire representation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := avro.Unmarshal(avro.MustParse(tt.schema), tt.data, tt.v)

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestUnmarshal_NestedZeroWidthArraysStayBounded(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":{"type":"array","items":"null"}}`)
	inner := append(avro.EncodeIntToBytes(500_000), 0x00)
	data := avro.EncodeIntToBytes(1000)
	for range 1000 {
		data = append(data, inner...)
	}

	var got [][]any
	allocated := allocatedBytes(func() {
		err := avro.Unmarshal(schema, data, &got)
		require.ErrorContains(t, err, "too many items without wire representation")
	})

	assert.Less(t, allocated, uint64(256<<20))
}

func TestUnmarshal_ElementBudgetIsCumulative(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":{"type":"array","items":"int"}},
		{"name":"b","type":{"type":"array","items":"int"}}
	]}`)
	api := avro.Config{MaxSliceAllocSize: 5}.Freeze()
	data := []byte{0x06, 0x02, 0x04, 0x06, 0x00, 0x06, 0x02, 0x04, 0x06, 0x00}

	var got struct {
		A []int `avro:"a"`
		B []int `avro:"b"`
	}
	err := api.Unmarshal(schema, data, &got)

	require.ErrorContains(t, err, "Config.MaxSliceAllocSize")
}

func TestDecoder_StreamingForgedCountAllocatesInChunks(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`)
	// OneByteReader hides the input length, as a network stream would.
	data := iotest.OneByteReader(bytes.NewReader(avro.EncodeIntToBytes(60_000_000)))
	dec := avro.NewDecoderForSchema(schema, data)

	var got []struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	allocated := allocatedBytes(func() {
		err := dec.Decode(&got)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	assert.Less(t, allocated, uint64(16<<20))
}

func TestDecoder_OCFStyleReaderUsesRemainingInput(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	dec := avro.NewDecoderForSchema(schema, bytes.NewBuffer(avro.EncodeIntToBytes(1_000_000)))

	var got []int64
	err := dec.Decode(&got)

	require.ErrorContains(t, err, "block count exceeds remaining input")
}

func TestUnmarshal_SkipForgedCountReturnsError(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name    string
		count   int
		wantErr string
	}{
		{name: "over slice limit", count: 1 << 62, wantErr: "Config.MaxSliceAllocSize"},
		{name: "zero width items", count: 2_000_000, wantErr: "too many items without wire representation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := avro.MustParse(`{"type":"record","name":"R","fields":[
				{"name":"e","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}
			]}`)
			data := append(avro.EncodeIntToBytes(tt.count), 0x00)

			var got otherFieldOnly
			err := avro.Unmarshal(schema, data, &got)

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestUnmarshal_SkipNullField(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"n","type":"null"},{"name":"other","type":"string"}]}`)

	var got otherFieldOnly
	err := avro.Unmarshal(schema, []byte{0x06, 'f', 'o', 'o'}, &got)

	require.NoError(t, err)
	assert.Equal(t, "foo", got.Other)
}

func TestReader_ReadNextForgedCountReturnsError(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name    string
		schema  string
		data    []byte
		wantErr string
	}{
		{name: "array of null", schema: `{"type":"array","items":"null"}`, data: avro.EncodeIntToBytes(40_000_000), wantErr: "too many items without wire representation"},
		{name: "array over slice limit", schema: `{"type":"array","items":"null"}`, data: avro.EncodeIntToBytes(1_000_000_000), wantErr: "Config.MaxSliceAllocSize"},
		{name: "map over remaining input", schema: `{"type":"map","values":"null"}`, data: avro.EncodeIntToBytes(40_000_000), wantErr: "block count exceeds remaining input"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.NewReader(nil, 0).Reset(tt.data)

			_ = r.ReadNext(avro.MustParse(tt.schema))

			require.ErrorContains(t, r.Error, tt.wantErr)
		})
	}
}

func TestReader_ReadNextNull(t *testing.T) {
	r := avro.NewReader(nil, 0).Reset(nil)

	got := r.ReadNext(avro.MustParse(`"null"`))

	require.NoError(t, r.Error)
	assert.Nil(t, got)
}

func TestDecoder_TruncatedValueReturnsUnexpectedEOF(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`)
	data := []byte{0x02, 0x06, 'f', 'o', 'o', 0x04, 0x06, 'b'}
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	var got struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	require.NoError(t, dec.Decode(&got))
	assert.Equal(t, "foo", got.B)

	err := dec.Decode(&got)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, err, io.EOF)
}

func TestDecoder_CleanBoundaryReturnsEOF(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`"long"`)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader([]byte{0x02}))

	var got int64
	require.NoError(t, dec.Decode(&got))
	assert.Equal(t, io.EOF, dec.Decode(&got))
}

func TestUnmarshal_MapUnmarshalerStopsOnError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	data := append(avro.EncodeIntToBytes(50_000_000), 0x00)
	dec := avro.NewDecoderForSchema(schema, iotest.OneByteReader(bytes.NewReader(data)))

	var got map[*netip.Addr]int
	err := decodeWithTimeout(t, dec, &got)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.LessOrEqual(t, len(got), 1)
}

func TestUnmarshal_MapUnmarshalerOverRemainingInput(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	data := []byte{0x80, 0xc2, 0xd7, 0x2f, 0x00, 0x00}

	var got map[*netip.Addr]int
	err := avro.Unmarshal(schema, data, &got)

	require.ErrorContains(t, err, "block count exceeds remaining input")
}

func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}
