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

type textPtrUnmarshalInt int

func (*textPtrUnmarshalInt) UnmarshalText([]byte) error { return nil }

type ptrMethodInt int

func (*ptrMethodInt) Reset() {}

type ptrUnionConverter struct{}

func (*ptrUnionConverter) FromAny(any) error   { return nil }
func (*ptrUnionConverter) ToAny() (any, error) { return nil, nil }

func TestEncodeExposesAddress(t *testing.T) {
	assert.False(t, encodeExposesAddress(reflect.TypeFor[int]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[textValInt]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[*textPtrInt]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[[]textPtrInt]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[encoding.TextMarshaler]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[ptrMethodInt]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[textPtrUnmarshalInt]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[time.Time]()))
	assert.False(t, encodeExposesAddress(reflect.TypeFor[struct{ T time.Time }]()))
	assert.True(t, encodeExposesAddress(reflect.TypeFor[textPtrInt]()))
	assert.True(t, encodeExposesAddress(reflect.TypeFor[[2]textPtrInt]()))
	assert.True(t, encodeExposesAddress(reflect.TypeFor[struct{ A textPtrInt }]()))
	assert.True(t, encodeExposesAddress(reflect.TypeFor[ptrUnionConverter]()))
}

func TestDecodeExposesAddress(t *testing.T) {
	assert.False(t, decodeExposesAddress(reflect.TypeFor[int]()))
	assert.False(t, decodeExposesAddress(reflect.TypeFor[textPtrInt]()))
	assert.False(t, decodeExposesAddress(reflect.TypeFor[ptrMethodInt]()))
	assert.False(t, decodeExposesAddress(reflect.TypeFor[*textPtrUnmarshalInt]()))
	assert.True(t, decodeExposesAddress(reflect.TypeFor[textPtrUnmarshalInt]()))
	assert.True(t, decodeExposesAddress(reflect.TypeFor[struct{ A [1]textPtrUnmarshalInt }]()))
	assert.True(t, decodeExposesAddress(reflect.TypeFor[time.Time]()))
	assert.True(t, decodeExposesAddress(reflect.TypeFor[ptrUnionConverter]()))
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
