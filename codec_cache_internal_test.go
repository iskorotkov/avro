package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodecCache_StopsAtLimit(t *testing.T) {
	var c codecCache

	c.store(cacheKey{rtype: 1}, 1, 2)
	c.store(cacheKey{rtype: 1}, 10, 2)
	c.store(cacheKey{rtype: 2}, 2, 2)
	c.store(cacheKey{rtype: 3}, 3, 2)

	assert.Equal(t, 1, c.load(cacheKey{rtype: 1}))
	assert.Equal(t, 2, c.load(cacheKey{rtype: 2}))
	assert.Nil(t, c.load(cacheKey{rtype: 3}))
}

func TestCodecCache_UnboundedWithoutLimit(t *testing.T) {
	var c codecCache

	for i := range 100 {
		c.store(cacheKey{rtype: uintptr(i)}, i, 0)
	}

	assert.Equal(t, int64(100), c.size.Load())
}
