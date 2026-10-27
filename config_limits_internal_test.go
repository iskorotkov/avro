package avro

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
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

func config(c Config) *frozenConfig {
	return c.Freeze().(*frozenConfig)
}
