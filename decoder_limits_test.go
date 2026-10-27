package avro_test

import (
	"bytes"
	"fmt"
	"io"
	"net/netip"
	"runtime"
	"strings"
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

			require.ErrorIs(t, err, avro.ErrMaxDepth)
		})
	}
}

func TestReader_ReadNextDeepRecursionReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	r := avro.NewReader(bytes.NewReader(linkedListPayload(3_000_000)), 1024)

	_ = r.ReadNext(schema)

	require.ErrorIs(t, r.Error, avro.ErrMaxDepth)
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
	require.ErrorIs(t, api.Unmarshal(schema, linkedListPayload(5), &got), avro.ErrMaxDepth)
}

// linkedList builds a list of n nodes.
func linkedList(n int) *linkedNode {
	node := &linkedNode{}
	for range n - 1 {
		node = &linkedNode{Next: node}
	}
	return node
}

func TestMaxDepth_CountsRecordsInEveryForm(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	api := avro.Config{MaxDepth: 5}.Freeze()

	tests := []struct {
		name string
		code func(nodes int) error
	}{
		{name: "encode pointer", code: func(nodes int) error {
			_, err := api.Marshal(schema, linkedList(nodes))
			return err
		}},
		{name: "encode value", code: func(nodes int) error {
			_, err := api.Marshal(schema, *linkedList(nodes))
			return err
		}},
		{name: "decode struct", code: func(nodes int) error {
			return api.Unmarshal(schema, linkedListPayload(nodes-1), &linkedNode{})
		}},
		{name: "decode pointer", code: func(nodes int) error {
			var got *linkedNode
			return api.Unmarshal(schema, linkedListPayload(nodes-1), &got)
		}},
		{name: "decode interface holding pointer", code: func(nodes int) error {
			var got any = &linkedNode{}
			return api.Unmarshal(schema, linkedListPayload(nodes-1), &got)
		}},
		{name: "decode generic", code: func(nodes int) error {
			var got any
			return api.Unmarshal(schema, linkedListPayload(nodes-1), &got)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.code(5))
			require.ErrorIs(t, tt.code(6), avro.ErrMaxDepth)
		})
	}
}

func TestMarshal_CyclicValueReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	node := &linkedNode{}
	node.Next = node

	_, err := avro.Marshal(schema, node)

	require.ErrorIs(t, err, avro.ErrMaxDepth)
}

func TestMarshal_CyclicGenericValueReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"M","fields":[{"name":"m","type":{"type":"map","values":["null","M"]}}]}`)
	inner := map[string]any{}
	value := map[string]any{"m": inner}
	inner["self"] = map[string]any{"M": value}

	_, err := avro.Marshal(schema, value)

	require.ErrorIs(t, err, avro.ErrMaxDepth)
}

func TestUnmarshal_ForgedBlockCountsReturnError(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name    string
		schema  string
		data    []byte
		v       any
		wantErr error
	}{
		{
			name:    "array over slice limit",
			schema:  `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`,
			data:    avro.EncodeIntToBytes(1_000_000_000),
			v:       new([]struct{ A int64 }),
			wantErr: avro.ErrMaxSliceAllocSize,
		},
		{
			name:    "array over remaining input",
			schema:  `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new([]struct{ A int64 }),
			wantErr: avro.ErrBlockCountExceedsInput,
		},
		{
			name:    "scalar array over remaining input",
			schema:  `{"type":"array","items":"double"}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new([]float64),
			wantErr: avro.ErrBlockCountExceedsInput,
		},
		{
			name:    "map over remaining input",
			schema:  `{"type":"map","values":"int"}`,
			data:    avro.EncodeIntToBytes(100_000_000),
			v:       new(map[string]int),
			wantErr: avro.ErrMaxMapAllocSize,
		},
		{
			name:    "map into any over remaining input",
			schema:  `{"type":"map","values":"null"}`,
			data:    avro.EncodeIntToBytes(1_000_000),
			v:       new(any),
			wantErr: avro.ErrBlockCountExceedsInput,
		},
		{
			name:    "zero width items",
			schema:  `{"type":"array","items":"null"}`,
			data:    avro.EncodeIntToBytes(2_000_000),
			v:       new([]any),
			wantErr: avro.ErrTooManyZeroWidthItems,
		},
		{
			name:    "zero width items into any",
			schema:  `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
			data:    avro.EncodeIntToBytes(2_000_000),
			v:       new(any),
			wantErr: avro.ErrTooManyZeroWidthItems,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := avro.Unmarshal(avro.MustParse(tt.schema), tt.data, tt.v)

			require.ErrorIs(t, err, tt.wantErr)
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
		require.ErrorIs(t, err, avro.ErrTooManyZeroWidthItems)
	})

	assert.Less(t, allocated, uint64(256<<20))
}

func TestUnmarshal_ElementBudgetIsCumulative(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name    string
		schema  string
		api     avro.API
		v       any
		wantErr error
	}{
		{
			name:   "arrays at limit",
			schema: `{"type":"array","items":"int"}`,
			api:    avro.Config{MaxSliceAllocSize: 6}.Freeze(),
			v:      new(struct{ A, B []int }),
		},
		{
			name:    "arrays over limit",
			schema:  `{"type":"array","items":"int"}`,
			api:     avro.Config{MaxSliceAllocSize: 5}.Freeze(),
			v:       new(struct{ A, B []int }),
			wantErr: avro.ErrMaxSliceAllocSize,
		},
		{
			name:   "maps at limit",
			schema: `{"type":"map","values":"int"}`,
			api:    avro.Config{MaxMapAllocSize: 6}.Freeze(),
			v:      new(struct{ A, B map[string]int }),
		},
		{
			name:    "maps over limit",
			schema:  `{"type":"map","values":"int"}`,
			api:     avro.Config{MaxMapAllocSize: 5}.Freeze(),
			v:       new(struct{ A, B map[string]int }),
			wantErr: avro.ErrMaxMapAllocSize,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"A","type":` + tt.schema + `},{"name":"B","type":` + tt.schema + `}]}`)
			// Each field holds three items: array items 1, 2, 3 or map entries "a"->1, "b"->2, "c"->3.
			block := []byte{0x06, 0x02, 0x04, 0x06, 0x00}
			if strings.Contains(tt.schema, "map") {
				block = []byte{0x06, 0x02, 'a', 0x02, 0x02, 'b', 0x04, 0x02, 'c', 0x06, 0x00}
			}

			err := tt.api.Unmarshal(schema, append(block, block...), tt.v)

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// panickingKey panics when unmarshaling the key "boom", like a buggy user type.
type panickingKey string

func (k *panickingKey) UnmarshalText(b []byte) error {
	if string(b) == "boom" {
		panic("boom")
	}
	*k = panickingKey(b)
	return nil
}

func TestReader_RecoveredPanicDoesNotLeakBudget(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	api := avro.Config{MaxMapAllocSize: 2}.Freeze()
	first := []byte{0x04, 0x08, 'b', 'o', 'o', 'm'}
	second := []byte{0x04, 0x02, 'a', 0x02, 0x02, 'b', 0x04, 0x00}
	r := avro.NewReader(bytes.NewReader(append(first, second...)), 64, avro.WithReaderConfig(api))

	require.Panics(t, func() {
		var got map[*panickingKey]int
		r.ReadVal(schema, &got)
	})

	var got map[string]int
	r.ReadVal(schema, &got)

	require.NoError(t, r.Error)
	assert.Equal(t, map[string]int{"a": 1, "b": 2}, got)
}

// panickingText panics when marshaling "boom", like a buggy user type.
type panickingText string

func (p panickingText) MarshalText() ([]byte, error) {
	if p == "boom" {
		panic("boom")
	}
	return []byte(p), nil
}

type textNode struct {
	S    panickingText `avro:"s"`
	Next *textNode     `avro:"next"`
}

func TestEncoder_RecoveredPanicDoesNotLeakDepth(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"N","fields":[{"name":"s","type":"string"},{"name":"next","type":["null","N"]}]}`)
	enc := avro.Config{MaxDepth: 2}.Freeze().NewEncoder(schema, io.Discard)

	require.Panics(t, func() {
		_ = enc.Encode(&textNode{S: "a", Next: &textNode{S: "boom"}})
	})

	require.NoError(t, enc.Encode(&textNode{S: "a", Next: &textNode{S: "b"}}))
}

func TestUnmarshal_ForgedBlockCountWrapsUnexpectedEOF(t *testing.T) {
	defer ConfigTeardown()

	var got []int64
	err := avro.Unmarshal(avro.MustParse(`{"type":"array","items":"long"}`), avro.EncodeIntToBytes(1000), &got)

	require.ErrorIs(t, err, avro.ErrBlockCountExceedsInput)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestDecoder_StreamingForgedCountAllocatesInChunks(t *testing.T) {
	defer ConfigTeardown()

	const count = 60_000_000
	tests := []struct {
		name   string
		schema string
		v      any
	}{
		{
			name:   "record array",
			schema: `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}}`,
			v: new([]struct {
				A int64  `avro:"a"`
				B string `avro:"b"`
			}),
		},
		{name: "long array", schema: `{"type":"array","items":"long"}`, v: new([]int64)},
		{name: "double array", schema: `{"type":"array","items":"double"}`, v: new([]float64)},
		{name: "map", schema: `{"type":"map","values":"int"}`, v: new(map[string]int)},
		{name: "map with unmarshaler keys", schema: `{"type":"map","values":"int"}`, v: new(map[*netip.Addr]int)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// OneByteReader hides the input length, as a network stream would.
			data := iotest.OneByteReader(bytes.NewReader(avro.EncodeIntToBytes(count)))
			dec := avro.NewDecoderForSchema(avro.MustParse(tt.schema), data)

			allocated := allocatedBytes(func() {
				err := dec.Decode(tt.v)
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			})

			assert.Less(t, allocated, uint64(16<<20))
		})
	}
}

func TestReader_ReadMapCBForgedCountOverSizedReaderReturnsError(t *testing.T) {
	defer ConfigTeardown()

	r := avro.NewReader(bytes.NewReader(append(avro.EncodeIntToBytes(1000), 0x02, 'k', 0x02)), 16)

	calls := 0
	r.ReadMapCB(func(r *avro.Reader, _ string) bool {
		calls++
		_ = r.ReadInt()
		return true
	})

	require.ErrorIs(t, r.Error, avro.ErrBlockCountExceedsInput)
	assert.Zero(t, calls)
}

func TestDecoder_OCFStyleReaderUsesRemainingInput(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":"long"}`)
	dec := avro.NewDecoderForSchema(schema, bytes.NewBuffer(avro.EncodeIntToBytes(1_000_000)))

	var got []int64
	err := dec.Decode(&got)

	require.ErrorIs(t, err, avro.ErrBlockCountExceedsInput)
}

func TestUnmarshal_SkipForgedCountReturnsError(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"e","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}
	]}`)
	tests := []struct {
		name    string
		api     avro.API
		count   int
		wantErr error
	}{
		{name: "at slice limit", api: avro.Config{MaxSliceAllocSize: 1000}.Freeze(), count: 1000},
		{name: "over slice limit", api: avro.Config{MaxSliceAllocSize: 1000}.Freeze(), count: 1001, wantErr: avro.ErrMaxSliceAllocSize},
		{name: "zero width items", api: avro.DefaultConfig, count: 2_000_000, wantErr: avro.ErrTooManyZeroWidthItems},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append(avro.EncodeIntToBytes(tt.count), 0x00)

			var got otherFieldOnly
			err := tt.api.Unmarshal(schema, data, &got)

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestUnmarshal_SkipCountOverRemainingInputReturnsError(t *testing.T) {
	defer ConfigTeardown()

	tests := []struct {
		name  string
		field string
	}{
		{name: "array", field: `{"type":"array","items":"long"}`},
		{name: "map", field: `{"type":"map","values":"long"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"s","type":` + tt.field + `}]}`)
			// Three items need at least three bytes for an array and six for a map, but only two follow.
			data := []byte{0x06, 0x02, 0x02}

			var got otherFieldOnly
			err := avro.Unmarshal(schema, data, &got)

			require.ErrorIs(t, err, avro.ErrBlockCountExceedsInput)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
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
		wantErr error
	}{
		{name: "array of null", schema: `{"type":"array","items":"null"}`, data: avro.EncodeIntToBytes(40_000_000), wantErr: avro.ErrTooManyZeroWidthItems},
		{name: "array over slice limit", schema: `{"type":"array","items":"null"}`, data: avro.EncodeIntToBytes(1_000_000_000), wantErr: avro.ErrMaxSliceAllocSize},
		{name: "map over remaining input", schema: `{"type":"map","values":"null"}`, data: avro.EncodeIntToBytes(40_000_000), wantErr: avro.ErrBlockCountExceedsInput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.NewReader(nil, 0).Reset(tt.data)

			_ = r.ReadNext(avro.MustParse(tt.schema))

			require.ErrorIs(t, r.Error, tt.wantErr)
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
	reader := newCountingReader(append(avro.EncodeIntToBytes(50_000_000), 0x00))
	dec := avro.NewDecoderForSchema(schema, reader)

	var got map[*netip.Addr]int
	err := dec.Decode(&got)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.LessOrEqual(t, len(got), 1)
	assert.Less(t, reader.getCallCount(), int64(10))
}

func TestUnmarshal_MapUnmarshalerOverRemainingInput(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"map","values":"int"}`)
	data := []byte{0x80, 0xc2, 0xd7, 0x2f, 0x00, 0x00}

	var got map[*netip.Addr]int
	err := avro.Unmarshal(schema, data, &got)

	require.ErrorIs(t, err, avro.ErrBlockCountExceedsInput)
}

func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// dagRecordSchema builds records Lk with two fields of type L(k-1) over a leaf field, so a decoder that keeps going after an error does 2^levels work.
func dagRecordSchema(levels int, leaf string) string {
	schema := `{"type":"record","name":"L0","fields":[{"name":"v","type":` + leaf + `}]}`
	for k := 1; k <= levels; k++ {
		schema = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"a","type":%s},{"name":"b","type":"L%d"}]}`, k, schema, k-1)
	}
	return schema
}

// dagNode matches every level of dagRecordSchema.
type dagNode struct {
	V int64    `avro:"v"`
	A *dagNode `avro:"a"`
	B *dagNode `avro:"b"`
}

func TestUnmarshal_TruncatedDAGStopsAtFirstError(t *testing.T) {
	defer ConfigTeardown()

	// A decoder that keeps going after the first error allocates for each of the 2^20 nodes.
	schema := avro.MustParse(dagRecordSchema(20, `"long"`))
	data := []byte{0x02}

	tests := []struct {
		name   string
		decode func() error
	}{
		{name: "struct", decode: func() error {
			return avro.Unmarshal(schema, data, &dagNode{})
		}},
		{name: "any", decode: func() error { return avro.Unmarshal(schema, data, new(any)) }},
		{name: "skip", decode: func() error { return avro.Unmarshal(schema, data, &struct{}{}) }},
		{name: "ReadNext", decode: func() error {
			r := avro.NewReader(nil, 0).Reset(data)
			_ = r.ReadNext(schema)
			return r.Error
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			allocs := testing.AllocsPerRun(1, func() { err = tt.decode() })

			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			assert.Less(t, allocs, float64(200))
		})
	}
}

func TestDecoder_TruncatedDAGSkipStopsAtFirstError(t *testing.T) {
	defer ConfigTeardown()

	// Skipping does not allocate, so count reads: each fixed leaf skipped after EOF reads the stream again.
	reader := newCountingReader([]byte{0x02})
	dec := avro.NewDecoderForSchema(avro.MustParse(dagRecordSchema(20, `{"type":"fixed","name":"F","size":1}`)), reader)

	err := dec.Decode(&struct{}{})

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Less(t, reader.getCallCount(), int64(10))
}

func TestUnmarshal_DepthErrorIsNotRepeatedPerLevel(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)

	var got linkedNode
	err := avro.Unmarshal(schema, linkedListPayload(5000), &got)

	require.ErrorIs(t, err, avro.ErrMaxDepth)
	assert.Less(t, len(err.Error()), 512)
}

func TestMarshal_DepthErrorIsNotRepeatedPerLevel(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(linkedListSchema)
	node := &linkedNode{}
	node.Next = node

	_, err := avro.Marshal(schema, node)

	require.ErrorIs(t, err, avro.ErrMaxDepth)
	assert.Less(t, len(err.Error()), 512)
}

func TestReader_ReadArrayCBIsNotChargedToElementBudget(t *testing.T) {
	defer ConfigTeardown()

	api := avro.Config{MaxSliceAllocSize: 2}.Freeze()
	r := avro.NewReader(nil, 0, avro.WithReaderConfig(api)).Reset([]byte{0x06, 0x02, 0x04, 0x06, 0x00})

	var got []int32
	r.ReadArrayCB(func(r *avro.Reader) bool {
		got = append(got, r.ReadInt())
		return true
	})

	require.NoError(t, r.Error)
	assert.Equal(t, []int32{1, 2, 3}, got)
}

func TestReader_ReadNextTruncatedReturnsUnexpectedEOF(t *testing.T) {
	r := avro.NewReader(nil, 0).Reset([]byte{0x02, 0x06, 'f'})

	_ = r.ReadNext(avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`))

	require.ErrorIs(t, r.Error, io.ErrUnexpectedEOF)
	assert.NotErrorIs(t, r.Error, io.EOF)
}

func TestReader_ReadNextAtCleanEndReturnsEOF(t *testing.T) {
	r := avro.NewReader(bytes.NewReader([]byte{0x02}), 16)
	schema := avro.MustParse(`"long"`)

	assert.Equal(t, int64(1), r.ReadNext(schema))
	require.NoError(t, r.Error)

	_ = r.ReadNext(schema)
	assert.Equal(t, io.EOF, r.Error)
}

// availableNowReader reports only the bytes available right now, which is not the remaining input.
type availableNowReader struct {
	io.Reader
}

func (availableNowReader) Len() int { return 0 }

func TestDecoder_UnknownLenReaderIsTreatedAsStream(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{"type":"array","items":"int"}`)
	dec := avro.NewDecoderForSchema(schema, availableNowReader{bytes.NewReader([]byte{0x04, 0x02, 0x04, 0x00})})

	var got []int
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, got)
}

func TestUnmarshal_DefaultSliceLimitBoundary(t *testing.T) {
	defer ConfigTeardown()

	const limit = 64 << 20
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"boolean"}}]}`)
	// Booleans take one byte each, so the input holds every counted item and only the element limit applies.
	payload := func(n int) []byte {
		return append(append(avro.EncodeIntToBytes(n), make([]byte, n)...), 0x00)
	}

	var got otherFieldOnly
	require.NoError(t, avro.Unmarshal(schema, payload(limit), &got))
	require.ErrorIs(t, avro.Unmarshal(schema, payload(limit+1), &got), avro.ErrMaxSliceAllocSize)
}
