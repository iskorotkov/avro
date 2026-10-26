package avro

import (
	"sync"
	"sync/atomic"
)

// codecCache is a concurrent map that stops accepting new keys once it holds limit entries.
type codecCache struct {
	entries sync.Map // map[cacheKey]any
	size    atomic.Int64
}

func (c *codecCache) load(key cacheKey) any {
	v, _ := c.entries.Load(key)
	return v
}

// store adds the value unless the key exists or a positive limit has been reached.
func (c *codecCache) store(key cacheKey, v any, limit int) {
	if limit > 0 && c.size.Load() >= int64(limit) {
		return
	}
	if _, loaded := c.entries.LoadOrStore(key, v); !loaded {
		c.size.Add(1)
	}
}
