package avro

// directReader reads one generic value straight into an interface without a typed scratch allocation.
type directReader func(r *Reader) any

// directReaderOf returns a directReader for plain primitives, or nil when the schema needs a full decoder.
func directReaderOf(schema Schema) directReader {
	prim, ok := schema.(*PrimitiveSchema)
	if !ok || prim.logical != nil || prim.encodedType != "" {
		return nil
	}

	switch prim.typ {
	case Boolean:
		return func(r *Reader) any { return r.ReadBool() }
	case Int:
		return func(r *Reader) any { return int(r.ReadInt()) }
	case Long:
		return func(r *Reader) any { return r.ReadLong() }
	case Float:
		return func(r *Reader) any { return r.ReadFloat() }
	case Double:
		return func(r *Reader) any { return r.ReadDouble() }
	case String:
		return func(r *Reader) any { return r.ReadString() }
	case Bytes:
		return func(r *Reader) any { return r.ReadBytes() }
	default:
		return nil
	}
}
