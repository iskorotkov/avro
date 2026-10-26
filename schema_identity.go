package avro

import (
	"encoding/json"
	"fmt"
)

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

// defaultsIdentity returns the field defaults of every record reachable from the schema.
func defaultsIdentity(schema Schema) []byte {
	return appendDefaults(nil, schema, map[NamedSchema]struct{}{})
}

func appendDefaults(identity []byte, schema Schema, seen map[NamedSchema]struct{}) []byte {
	if ref, ok := schema.(*RefSchema); ok {
		schema = ref.Schema()
	}
	if named, ok := schema.(NamedSchema); ok {
		if _, ok = seen[named]; ok {
			return identity
		}
		seen[named] = struct{}{}
	}
	if rec, ok := schema.(*RecordSchema); ok {
		identity = appendFieldDefaults(identity, rec)
	}
	for _, child := range schemaChildren(schema) {
		identity = appendDefaults(identity, child, seen)
	}
	return identity
}

func appendFieldDefaults(identity []byte, rec *RecordSchema) []byte {
	for _, f := range rec.Fields() {
		if !f.HasDefault() {
			continue
		}
		def, err := json.Marshal(f.Default())
		if err != nil {
			def = fmt.Appendf(nil, "%#v", f.Default())
		}
		identity = fmt.Appendf(identity, "%q.%q=%d:%s", rec.FullName(), f.Name(), len(def), def)
	}
	return identity
}
