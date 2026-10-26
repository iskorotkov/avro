package unsafex

import (
	"reflect"
	"unsafe"
)

type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

// zeroBase backs empty non-nil slices.
var zeroBase [0]uint64

// SliceLen returns the length of the slice at ptr.
func SliceLen(ptr unsafe.Pointer) int {
	return (*sliceHeader)(ptr).len
}

// SliceIndex returns a pointer to the i-th element of the slice at ptr.
func SliceIndex(ptr unsafe.Pointer, i int, elemSize uintptr) unsafe.Pointer {
	return unsafe.Add((*sliceHeader)(ptr).data, uintptr(i)*elemSize)
}

// SetEmptySlice makes the nil slice at ptr empty but non-nil without allocating.
func SetEmptySlice(ptr unsafe.Pointer) {
	h := (*sliceHeader)(ptr)
	if h.data == nil {
		h.data = unsafe.Pointer(&zeroBase)
	}
}

// GrowSlice sets the length of the slice of type t at ptr to n, reallocating when n exceeds its capacity.
func GrowSlice(t reflect.Type, ptr unsafe.Pointer, n int) {
	h := (*sliceHeader)(ptr)
	if n <= h.cap {
		h.len = n
		return
	}

	v := reflect.NewAt(t, ptr).Elem()
	v.Grow(n - h.len)
	v.SetLen(n)
}
