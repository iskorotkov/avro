package avro

// externalRefsIdentity returns fingerprints of named schemas the schema references but does not define.
func externalRefsIdentity(schema Schema) []byte {
	return appendExternalRefs(nil, schema, map[NamedSchema]struct{}{})
}

func appendExternalRefs(identity []byte, schema Schema, seen map[NamedSchema]struct{}) []byte {
	ref, isRef := schema.(*RefSchema)
	if isRef {
		schema = ref.Schema()
	}
	if named, ok := schema.(NamedSchema); ok {
		if _, ok = seen[named]; ok {
			return identity
		}
		seen[named] = struct{}{}
	}
	if isRef {
		fp := schema.Fingerprint()
		identity = append(identity, fp[:]...)
	}
	for _, child := range schemaChildren(schema) {
		identity = appendExternalRefs(identity, child, seen)
	}
	return identity
}
