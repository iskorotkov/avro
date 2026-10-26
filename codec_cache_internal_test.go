package avro

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodecCache_EvictsWhenFull(t *testing.T) {
	var c codecCache

	c.store(cacheKey{rtype: 1}, 1, 2)
	c.store(cacheKey{rtype: 1}, 10, 2)
	c.store(cacheKey{rtype: 2}, 2, 2)
	c.store(cacheKey{rtype: 3}, 3, 2)

	assert.Equal(t, 3, c.load(cacheKey{rtype: 3}))
	assert.Equal(t, int64(2), c.size.Load())
	cached := 0
	for i := 1; i <= 2; i++ {
		if c.load(cacheKey{rtype: uintptr(i)}) != nil {
			cached++
		}
	}
	assert.Equal(t, 1, cached)
}

func TestCodecCache_UnboundedWithoutLimit(t *testing.T) {
	var c codecCache

	for i := range 100 {
		c.store(cacheKey{rtype: uintptr(i)}, i, 0)
	}

	assert.Equal(t, int64(100), c.size.Load())
}

func TestCodecCache_EvictOtherKeepsOnlyEntry(t *testing.T) {
	var c codecCache
	c.store(cacheKey{rtype: 1}, 1, 0)

	c.evictOther(cacheKey{rtype: 1})

	assert.Equal(t, 1, c.load(cacheKey{rtype: 1}))
	assert.Equal(t, int64(1), c.size.Load())
}

func TestCodecCache_EvictOtherRemovesOneEntry(t *testing.T) {
	var c codecCache
	for i := range 3 {
		c.store(cacheKey{rtype: uintptr(i)}, i, 0)
	}

	c.evictOther(cacheKey{rtype: 1})

	assert.Equal(t, 1, c.load(cacheKey{rtype: 1}))
	assert.Equal(t, int64(2), c.size.Load())
	assert.Equal(t, 2, cachedEntries(&c))
}

func TestCodecCache_ConcurrentStoresStayWithinLimit(t *testing.T) {
	const limit = 16
	var c codecCache

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 500 {
				key := cacheKey{rtype: uintptr(g*500 + i%100)}
				c.store(key, i, limit)
				c.load(key)
			}
		})
	}
	wg.Wait()

	assert.LessOrEqual(t, c.size.Load(), int64(limit))
	assert.Equal(t, int(c.size.Load()), cachedEntries(&c))
}

func cachedEntries(c *codecCache) int {
	n := 0
	c.entries.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
