package unsafex

import (
	"reflect"
	"unsafe"
)

// Boxer converts values stored in memory into interfaces without copying them.
type Boxer struct {
	typ    reflect.Type
	ptr    unsafe.Pointer
	direct bool
}

// NewBoxer returns a Boxer for values of type t.
func NewBoxer(t reflect.Type) Boxer {
	return Boxer{typ: t, ptr: TypePtr(t), direct: IsDirect(t)}
}

// Box returns the value at ptr as an interface; the result aliases the memory at ptr.
func (b Boxer) Box(ptr unsafe.Pointer) any {
	switch {
	case b.direct:
		return Pack(b.ptr, *(*unsafe.Pointer)(ptr))
	case b.typ.Kind() != reflect.Interface:
		return Pack(b.ptr, ptr)
	case b.typ.NumMethod() == 0:
		return *(*any)(ptr)
	default:
		return reflect.NewAt(b.typ, ptr).Elem().Interface()
	}
}
