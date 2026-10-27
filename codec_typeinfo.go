package avro

import "reflect"

// exposesAddress reports whether codecs may pass the address of a typ value, or of a value stored inline in it, to a user method.
func exposesAddress(typ reflect.Type) bool {
	return hasComponent(typ, map[reflect.Type]bool{}, func(t reflect.Type) bool {
		return t.Kind() != reflect.Interface && reflect.PointerTo(t).NumMethod() > t.NumMethod()
	})
}

// hasComponent reports whether match holds for typ or a type stored inline in it.
func hasComponent(typ reflect.Type, visited map[reflect.Type]bool, match func(reflect.Type) bool) bool {
	if visited[typ] {
		return false
	}
	visited[typ] = true

	if match(typ) {
		return true
	}

	switch typ.Kind() {
	case reflect.Array:
		return hasComponent(typ.Elem(), visited, match)
	case reflect.Struct:
		for field := range typ.Fields() {
			if hasComponent(field.Type, visited, match) {
				return true
			}
		}
	default:
	}

	return false
}
