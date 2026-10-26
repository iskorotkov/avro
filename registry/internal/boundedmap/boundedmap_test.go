package boundedmap_test

import (
	"sync"
	"testing"

	"github.com/iskorotkov/avro/v2/registry/internal/boundedmap"
	"github.com/stretchr/testify/assert"
)

func TestMap_LoadStoreDelete(t *testing.T) {
	m := boundedmap.New[int, string](0)

	m.Store(1, "a")
	v, ok := m.Load(1)
	assert.True(t, ok)
	assert.Equal(t, "a", v)

	m.Delete(1)
	_, ok = m.Load(1)
	assert.False(t, ok)
	assert.Equal(t, 0, m.Len())
}

func TestMap_UnboundedKeepsAll(t *testing.T) {
	m := boundedmap.New[int, int](0)

	for i := range 100 {
		m.Store(i, i)
	}

	assert.Equal(t, 100, m.Len())
}

func TestMap_OverwriteDoesNotGrow(t *testing.T) {
	m := boundedmap.New[int, int](2)

	m.Store(1, 1)
	m.Store(1, 2)

	v, _ := m.Load(1)
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, m.Len())
}

func TestMap_BoundEvictsOtherEntry(t *testing.T) {
	m := boundedmap.New[int, int](3)

	for i := range 10 {
		m.Store(i, i)
		_, ok := m.Load(i)
		assert.True(t, ok)
	}

	assert.Equal(t, 3, m.Len())
}

func TestMap_BoundHoldsUnderConcurrency(t *testing.T) {
	m := boundedmap.New[int, int](8)

	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 1000 {
				m.Store(g*1000+i, i)
			}
		})
	}
	wg.Wait()

	assert.LessOrEqual(t, m.Len(), 8)
}
