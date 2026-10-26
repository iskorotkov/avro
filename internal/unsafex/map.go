package unsafex

import (
	"reflect"
	"unsafe"
)

// MakeMap stores a new map of type t with room for n elements at ptr.
func MakeMap(t reflect.Type, ptr unsafe.Pointer, n int) {
	*(*unsafe.Pointer)(ptr) = reflect.MakeMapWithSize(t, n).UnsafePointer()
}

// MapOf returns the map of type t stored at ptr.
func MapOf(t reflect.Type, ptr unsafe.Pointer) reflect.Value {
	return reflect.NewAt(t, ptr).Elem()
}
