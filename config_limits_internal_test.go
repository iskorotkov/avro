package avro

import (
	"bytes"
	"math"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ZeroOrNegativeSelectsDefault(t *testing.T) {
	getters := []struct {
		name string
		get  func(n int) int
		want int
	}{
		{name: "MaxByteSliceSize", get: func(n int) int { return config(Config{MaxByteSliceSize: n}).getMaxByteSliceSize() }, want: defaultMaxByteSliceSize},
		{name: "MaxSliceAllocSize", get: func(n int) int { return config(Config{MaxSliceAllocSize: n}).getMaxSliceAllocSize() }, want: defaultMaxSliceAllocSize},
		{name: "MaxMapAllocSize", get: func(n int) int { return config(Config{MaxMapAllocSize: n}).getMaxMapAllocSize() }, want: defaultMaxMapAllocSize},
		{name: "MaxDepth", get: func(n int) int { return config(Config{MaxDepth: n}).getMaxDepth() }, want: defaultMaxDepth},
		{name: "BlockLength", get: func(n int) int { return config(Config{BlockLength: n}).getBlockLength() }, want: defaultBlockLength},
		{name: "SlabSize", get: func(n int) int { return config(Config{SlabSize: n}).getSlabSize() }, want: defaultSlabSize},
		{name: "ReadBufSize", get: func(n int) int { return config(Config{ReadBufSize: n}).getReadBufSize() }, want: defaultReadBufSize},
		{name: "WriteBufSize", get: func(n int) int { return config(Config{WriteBufSize: n}).getWriteBufSize() }, want: defaultWriteBufSize},
	}
	for _, g := range getters {
		t.Run(g.name, func(t *testing.T) {
			for _, n := range []int{0, -1, math.MinInt} {
				assert.Equal(t, g.want, g.get(n), "value %d", n)
			}
			assert.Equal(t, 1, g.get(1))
		})
	}
}

func TestConfig_LimitsAreClampedToMaxAllocSize(t *testing.T) {
	cfg := config(Config{MaxByteSliceSize: math.MaxInt, MaxSliceAllocSize: math.MaxInt, MaxMapAllocSize: math.MaxInt})

	assert.Equal(t, maxAllocSize, cfg.getMaxByteSliceSize())
	assert.Equal(t, maxAllocSize, cfg.getMaxSliceAllocSize())
	assert.Equal(t, maxAllocSize, cfg.getMaxMapAllocSize())
}

func TestReader_DefaultElementLimitsBoundary(t *testing.T) {
	tests := []struct {
		name    string
		allow   func(r *Reader, n int) bool
		limit   int
		wantErr error
	}{
		{name: "array", allow: func(r *Reader, n int) bool { return r.allowArrayBlock(n, 1) }, limit: 64 << 20, wantErr: ErrMaxSliceAllocSize},
		{name: "map", allow: func(r *Reader, n int) bool { return r.allowMapBlock(n, 1) }, limit: 64 << 20, wantErr: ErrMaxMapAllocSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A stream hides its length, so only the element budget applies.
			stream := func() *Reader { return NewReader(iotest.OneByteReader(bytes.NewReader(nil)), 0) }

			atLimit := stream()
			require.True(t, tt.allow(atLimit, tt.limit))
			require.NoError(t, atLimit.Error)

			overLimit := stream()
			require.False(t, tt.allow(overLimit, tt.limit+1))
			require.ErrorIs(t, overLimit.Error, tt.wantErr)
		})
	}
}

func TestReader_DefaultByteSliceLimitBoundary(t *testing.T) {
	read := func(n int) *Reader {
		w := NewWriter(nil, 0)
		w.WriteLong(int64(n))
		r := NewReader(nil, 0).Reset(append(w.Buffer(), make([]byte, n)...))
		_ = r.ReadBytes()
		return r
	}

	require.NoError(t, read(1<<20).Error)
	require.ErrorIs(t, read(1<<20+1).Error, ErrMaxByteSliceSize)
}

func config(c Config) *frozenConfig {
	return c.Freeze().(*frozenConfig)
}
