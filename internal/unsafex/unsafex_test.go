package unsafex_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypePtr_MatchesInterfaceTypeWord(t *testing.T) {
	values := []any{1, "s", []int{1}, map[string]int{}, &struct{}{}, struct{ A int }{1}}

	for _, v := range values {
		assert.Equal(t, unsafex.TypePtrOf(v), unsafex.TypePtr(reflect.TypeOf(v)))
	}
}

func TestPack_RoundTrips(t *testing.T) {
	v := any("hello")

	got := unsafex.Pack(unsafex.TypePtrOf(v), unsafex.DataOf(v))

	assert.Equal(t, v, got)
}

func TestIsNil(t *testing.T) {
	var nilPtr *int

	assert.True(t, unsafex.IsNil(nil))
	assert.True(t, unsafex.IsNil(nilPtr))
	assert.False(t, unsafex.IsNil(new(int)))
	assert.False(t, unsafex.IsNil(0))
}

func TestIsDirect_MatchesInterfaceLayout(t *testing.T) {
	p := new(int)
	values := []any{
		p,
		map[string]int{},
		make(chan int),
		func() {},
		unsafe.Pointer(p),
		struct{ P *int }{p},
		[1]*int{p},
		struct{ A [1]*int }{[1]*int{p}},
		1,
		"s",
		[]int{},
		struct{ A, B *int }{p, p},
		[2]*int{p, p},
	}

	for _, v := range values {
		// A direct interface stores the pointer itself, which then equals the first word of a copy.
		want := uintptr(unsafex.DataOf(v)) == firstWord(v)
		assert.Equal(t, want, unsafex.IsDirect(reflect.TypeOf(v)), "%T", v)
	}
}

func firstWord(v any) uintptr {
	cp := reflect.New(reflect.TypeOf(v))
	cp.Elem().Set(reflect.ValueOf(v))
	return *(*uintptr)(cp.UnsafePointer())
}

func TestBoxer_Box(t *testing.T) {
	s := "hello"
	p := &s
	var e any = 42
	var st interface{ String() string } = reflect.TypeFor[int]()

	assert.Equal(t, any("hello"), unsafex.NewBoxer(reflect.TypeFor[string]()).Box(unsafe.Pointer(&s)))
	assert.Equal(t, any(p), unsafex.NewBoxer(reflect.TypeFor[*string]()).Box(unsafe.Pointer(&p)))
	assert.Equal(t, any(42), unsafex.NewBoxer(reflect.TypeFor[any]()).Box(unsafe.Pointer(&e)))
	assert.Equal(t, any(reflect.TypeFor[int]()), unsafex.NewBoxer(reflect.TypeFor[interface{ String() string }]()).Box(unsafe.Pointer(&st)))
}

func TestNewAddrBoxer_BoxesPointer(t *testing.T) {
	s := "hello"

	got := unsafex.NewAddrBoxer(reflect.TypeFor[string]()).Box(unsafe.Pointer(&s))

	assert.Same(t, &s, got)
}

func TestSetEmptySlice(t *testing.T) {
	var s []int
	other := []int{1}

	unsafex.SetEmptySlice(unsafe.Pointer(&s))
	unsafex.SetEmptySlice(unsafe.Pointer(&other))

	assert.NotNil(t, s)
	assert.Empty(t, s)
	assert.Equal(t, []int{1}, other)
	assert.Equal(t, []int{2}, append(s, 2))
}

func TestGrowSlice(t *testing.T) {
	s := make([]int, 1, 2)
	s[0] = 7
	typ := reflect.TypeFor[[]int]()

	unsafex.GrowSlice(typ, unsafe.Pointer(&s), 2)
	require.Len(t, s, 2)
	unsafex.GrowSlice(typ, unsafe.Pointer(&s), 10)

	assert.Len(t, s, 10)
	assert.Equal(t, 7, s[0])
	assert.Equal(t, 10, unsafex.SliceLen(unsafe.Pointer(&s)))
	*(*int)(unsafex.SliceIndex(unsafe.Pointer(&s), 9, unsafe.Sizeof(0))) = 5
	assert.Equal(t, 5, s[9])
}

func TestMakeMap(t *testing.T) {
	var m map[string]int

	unsafex.MakeMap(reflect.TypeFor[map[string]int](), unsafe.Pointer(&m), 4)
	unsafex.MapOf(reflect.TypeFor[map[string]int](), unsafe.Pointer(&m)).SetMapIndex(reflect.ValueOf("a"), reflect.ValueOf(1))

	assert.Equal(t, map[string]int{"a": 1}, m)
}
