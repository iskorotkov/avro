package protobuf

// enum looks name up in a generated enum value map, panicking on unknown names.
func enum[E ~int32](values map[string]int32, name string) E {
	v, ok := values[name]
	if !ok {
		panic("protobuf: unknown enum value " + name)
	}

	return E(v)
}
