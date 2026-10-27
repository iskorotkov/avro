package avro

import (
	"encoding"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type textPtrInt int

func (*textPtrInt) MarshalText() ([]byte, error) { return nil, nil }

type textValInt int

func (textValInt) MarshalText() ([]byte, error) { return nil, nil }

func TestExposesAddress(t *testing.T) {
	assert.False(t, exposesAddress(reflect.TypeFor[int]()))
	assert.False(t, exposesAddress(reflect.TypeFor[textValInt]()))
	assert.False(t, exposesAddress(reflect.TypeFor[*textPtrInt]()))
	assert.False(t, exposesAddress(reflect.TypeFor[[]textPtrInt]()))
	assert.False(t, exposesAddress(reflect.TypeFor[encoding.TextMarshaler]()))
	assert.True(t, exposesAddress(reflect.TypeFor[textPtrInt]()))
	assert.True(t, exposesAddress(reflect.TypeFor[[2]textPtrInt]()))
	assert.True(t, exposesAddress(reflect.TypeFor[struct{ A textPtrInt }]()))
}

func TestAccumulates(t *testing.T) {
	type node struct {
		Next *node
		Val  int
	}

	assert.False(t, accumulates(reflect.TypeFor[int]()))
	assert.False(t, accumulates(reflect.TypeFor[*node]()))
	assert.False(t, accumulates(reflect.TypeFor[struct{ A []string }]()))
	assert.True(t, accumulates(reflect.TypeFor[map[string]int]()))
	assert.False(t, accumulates(reflect.TypeFor[any]()))
	assert.True(t, accumulates(reflect.TypeFor[[]struct{ M map[string]int }]()))
	assert.True(t, accumulates(reflect.TypeFor[*struct{ M map[string]int }]()))
}

func TestSameLayout(t *testing.T) {
	assert.True(t, sameLayout(reflect.TypeFor[int64](), reflect.TypeFor[uint64]()))
	assert.True(t, sameLayout(reflect.TypeFor[[]int](), reflect.TypeFor[[]byte]()))
	assert.True(t, sameLayout(reflect.TypeFor[*int](), reflect.TypeFor[unsafe.Pointer]()))
	assert.True(t, sameLayout(reflect.TypeFor[time.Time](), reflect.TypeFor[time.Time]()))
	assert.False(t, sameLayout(reflect.TypeFor[int32](), reflect.TypeFor[uint64]()))
	assert.False(t, sameLayout(reflect.TypeFor[string](), reflect.TypeFor[[2]uintptr]()))
}
