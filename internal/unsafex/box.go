package unsafex

import (
	"reflect"
	"unsafe"
)

type boxMode uint8

const (
	boxIndirect boxMode = iota
	boxDirect
	boxEface
	boxIface
)

// Boxer converts values stored in memory into interfaces without copying them.
type Boxer struct {
	typ  reflect.Type
	ptr  unsafe.Pointer
	mode boxMode
}

// NewBoxer returns a Boxer for values of type t.
func NewBoxer(t reflect.Type) Boxer {
	b := Boxer{typ: t, ptr: TypePtr(t)}
	switch {
	case IsDirect(t):
		b.mode = boxDirect
	case t.Kind() != reflect.Interface:
		b.mode = boxIndirect
	case t.NumMethod() == 0:
		b.mode = boxEface
	default:
		b.mode = boxIface
	}
	return b
}

// NewAddrBoxer returns a Boxer that boxes the address of values of type t as a *t.
func NewAddrBoxer(t reflect.Type) Boxer {
	pt := reflect.PointerTo(t)
	return Boxer{typ: pt, ptr: TypePtr(pt), mode: boxIndirect}
}

// Box returns the value at ptr as an interface; the result aliases the memory at ptr.
func (b Boxer) Box(ptr unsafe.Pointer) any {
	switch b.mode {
	case boxDirect:
		return Pack(b.ptr, *(*unsafe.Pointer)(ptr))
	case boxEface:
		return *(*any)(ptr)
	case boxIface:
		return reflect.NewAt(b.typ, ptr).Elem().Interface()
	default:
		return Pack(b.ptr, ptr)
	}
}
