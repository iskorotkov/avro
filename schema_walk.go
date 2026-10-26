package avro

func walkSchema(schema Schema, fn func(Schema) Schema) Schema {
	schema = fn(schema)

	switch s := schema.(type) {
	case *RecordSchema:
		for _, f := range s.Fields() {
			f.typ = walkSchema(f.typ, fn)
		}
	case *ArraySchema:
		s.items = walkSchema(s.items, fn)
	case *MapSchema:
		s.values = walkSchema(s.values, fn)
	case *UnionSchema:
		for i, st := range s.types {
			s.types[i] = walkSchema(st, fn)
		}
	}
	return schema
}

// schemaChildren returns the direct child schemas without modifying the schema.
func schemaChildren(schema Schema) []Schema {
	switch s := schema.(type) {
	case *RecordSchema:
		children := make([]Schema, 0, len(s.Fields()))
		for _, f := range s.Fields() {
			children = append(children, f.Type())
		}
		return children
	case *ArraySchema:
		return []Schema{s.Items()}
	case *MapSchema:
		return []Schema{s.Values()}
	case *UnionSchema:
		return s.Types()
	default:
		return nil
	}
}
