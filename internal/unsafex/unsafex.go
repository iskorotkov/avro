// Package unsafex implements interface and slice memory layout helpers without runtime linkname.
package unsafex

import (
	"reflect"
	"unsafe"
)

type eface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// TypePtr returns the runtime type descriptor of t, as stored in the type word of an interface.
func TypePtr(t reflect.Type) unsafe.Pointer {
	return (*eface)(unsafe.Pointer(&t)).data
}

// TypePtrOf returns the type word of v.
func TypePtrOf(v any) unsafe.Pointer {
	return (*eface)(unsafe.Pointer(&v)).typ
}

// DataOf returns the data word of v.
func DataOf(v any) unsafe.Pointer {
	return (*eface)(unsafe.Pointer(&v)).data
}

// Pack returns an interface with the given type and data words.
func Pack(typ, data unsafe.Pointer) any {
	var v any
	e := (*eface)(unsafe.Pointer(&v))
	e.typ = typ
	e.data = data
	return v
}

// IsNil reports whether v is nil or holds a nil data word.
func IsNil(v any) bool {
	return v == nil || DataOf(v) == nil
}

// IsDirect reports whether interfaces store values of t in their data word instead of pointing to them.
func IsDirect(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return t.Len() == 1 && IsDirect(t.Elem())
	case reflect.Struct:
		return t.NumField() == 1 && IsDirect(t.Field(0).Type)
	default:
		return false
	}
}
