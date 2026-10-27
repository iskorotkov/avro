package avro

import "reflect"

// exposesAddress reports whether codecs may pass the address of a typ value, or of a value stored inline in it, to a user method.
func exposesAddress(typ reflect.Type) bool {
	return hasComponent(typ, false, map[reflect.Type]bool{}, func(t reflect.Type) bool {
		return t.Kind() != reflect.Interface && reflect.PointerTo(t).NumMethod() > t.NumMethod()
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
