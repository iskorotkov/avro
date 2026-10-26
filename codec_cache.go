package avro

import (
	"sync"
	"sync/atomic"
)

// codecCache is a concurrent map that evicts an arbitrary entry when a store exceeds its limit.
type codecCache struct {
	entries sync.Map // map[cacheKey]any
	size    atomic.Int64
}

func (c *codecCache) load(key cacheKey) any {
	v, _ := c.entries.Load(key)
	return v
}

// store adds the value unless the key exists, evicting another entry when a positive limit is exceeded.
func (c *codecCache) store(key cacheKey, v any, limit int) {
	if _, loaded := c.entries.LoadOrStore(key, v); loaded {
		return
	}
	if c.size.Add(1) > int64(limit) && limit > 0 {
		c.evictOther(key)
	}
}

func (c *codecCache) evictOther(keep cacheKey) {
	c.entries.Range(func(k, _ any) bool {
		if k == keep {
			return true
		}
		if _, deleted := c.entries.LoadAndDelete(k); deleted {
			c.size.Add(-1)
			return false
		}
		return true
	})
}
