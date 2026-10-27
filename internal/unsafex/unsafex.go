// Package unsafex implements interface and slice memory layout helpers without runtime linkname.
package unsafex

import (
	"reflect"
	"sync"
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

// directTypes caches IsDirect results by type.
var directTypes sync.Map // map[reflect.Type]bool

// IsDirect reports whether interfaces store values of t in their data word instead of pointing to them.
func IsDirect(t reflect.Type) bool {
	if direct, ok := directTypes.Load(t); ok {
		return direct.(bool)
	}

	// A boxed zero value has a nil data word only when the value itself is stored there.
	direct := t.Kind() != reflect.Interface && DataOf(reflect.Zero(t).Interface()) == nil
	directTypes.Store(t, direct)
	return direct
}
