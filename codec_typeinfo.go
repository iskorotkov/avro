package avro

import (
	"reflect"
	"slices"
)

// encodeExposesAddress reports whether encoders may pass the address of a typ value, or of a value stored inline in it, to a user method.
func encodeExposesAddress(typ reflect.Type) bool {
	return exposesAddressTo(typ, textMarshalerType, textAppenderType, unionConverterType)
}

// decodeExposesAddress reports whether decoders may pass the address of a typ value, or of a value stored inline in it, to a user method.
func decodeExposesAddress(typ reflect.Type) bool {
	return exposesAddressTo(typ, textUnmarshalerType, unionConverterType)
}

// exposesAddressTo reports whether typ stores inline a value implementing one of ifaces only through its address.
func exposesAddressTo(typ reflect.Type, ifaces ...reflect.Type) bool {
	return hasComponent(typ, false, map[reflect.Type]bool{}, func(t reflect.Type) bool {
		if t.Kind() == reflect.Interface {
			return false
		}

		ptr := reflect.PointerTo(t)
		return slices.ContainsFunc(ifaces, func(iface reflect.Type) bool {
			return !t.Implements(iface) && ptr.Implements(iface)
		})
	})
}

// accumulates reports whether decoding into an existing typ value can keep data from that value.
func accumulates(typ reflect.Type) bool {
	return hasComponent(typ, true, map[reflect.Type]bool{}, func(t reflect.Type) bool {
		return t.Kind() == reflect.Map
	})
}

// hasComponent reports whether match holds for typ or a type stored in it, following slices and pointers when refs is set.
func hasComponent(typ reflect.Type, refs bool, visited map[reflect.Type]bool, match func(reflect.Type) bool) bool {
	if visited[typ] {
		return false
	}
	visited[typ] = true

	if match(typ) {
		return true
	}

	switch typ.Kind() {
	case reflect.Array:
		return hasComponent(typ.Elem(), refs, visited, match)
	case reflect.Slice, reflect.Pointer:
		return refs && hasComponent(typ.Elem(), refs, visited, match)
	case reflect.Struct:
		for field := range typ.Fields() {
			if hasComponent(field.Type, refs, visited, match) {
				return true
			}
		}
	default:
	}

	return false
}

// sameLayout reports whether a and b share size, alignment and pointer-freedom.
func sameLayout(a, b reflect.Type) bool {
	return a.Size() == b.Size() && a.Align() == b.Align() && a.FieldAlign() == b.FieldAlign() &&
		pointerFree(a) == pointerFree(b)
}

// pointerFree reports whether values of typ contain no pointers.
func pointerFree(typ reflect.Type) bool {
	return !hasComponent(typ, false, map[reflect.Type]bool{}, func(t reflect.Type) bool {
		switch t.Kind() {
		case reflect.String, reflect.Slice, reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func,
			reflect.UnsafePointer, reflect.Interface:
			return true
		default:
			return false
		}
	})
}
