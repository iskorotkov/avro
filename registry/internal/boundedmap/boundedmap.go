// Package boundedmap provides a concurrent map with an optional size bound.
package boundedmap

import (
	"sync"
	"sync/atomic"
)

// Map is a concurrent map that evicts an arbitrary entry once it grows past its bound.
type Map[K comparable, V any] struct {
	m    sync.Map
	size atomic.Int64
	max  int64
}

// New returns a map holding at most maxLen entries; maxLen <= 0 means unbounded.
func New[K comparable, V any](maxLen int) *Map[K, V] {
	return &Map[K, V]{max: int64(maxLen)}
}

// Load returns the value stored for key.
func (b *Map[K, V]) Load(key K) (V, bool) {
	v, ok := b.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// Store sets the value for key, evicting another entry if the bound is exceeded.
func (b *Map[K, V]) Store(key K, value V) {
	if _, loaded := b.m.Swap(key, value); loaded {
		return
	}
	b.size.Add(1)
	for b.max > 0 && b.size.Load() > b.max && b.evictOther(key) {
	}
}

// Delete removes key.
func (b *Map[K, V]) Delete(key K) {
	if _, ok := b.m.LoadAndDelete(key); ok {
		b.size.Add(-1)
	}
}

// Len returns the approximate number of entries.
func (b *Map[K, V]) Len() int {
	return int(b.size.Load())
}

// evictOther deletes one entry other than keep and reports whether it did.
func (b *Map[K, V]) evictOther(keep K) bool {
	evicted := false
	b.m.Range(func(k, _ any) bool {
		if k.(K) == keep {
			return true
		}
		if _, ok := b.m.LoadAndDelete(k); ok {
			b.size.Add(-1)
			evicted = true
		}
		return !evicted
	})
	return evicted
}
