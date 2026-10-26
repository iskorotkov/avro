package avro

// AppendMarshaler is implemented by APIs that can encode directly into a caller-owned buffer.
type AppendMarshaler interface {
	// AppendMarshal appends the Avro encoding of v to dst and returns the extended buffer.
	AppendMarshal(dst []byte, schema Schema, v any) ([]byte, error)
}

// AppendMarshalWith appends the Avro encoding of v to dst using api.
// APIs that do not implement AppendMarshaler fall back to Marshal and an extra copy.
func AppendMarshalWith(api API, dst []byte, schema Schema, v any) ([]byte, error) {
	if am, ok := api.(AppendMarshaler); ok {
		return am.AppendMarshal(dst, schema, v)
	}

	b, err := api.Marshal(schema, v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// AppendMarshal appends the Avro encoding of v to dst and returns the extended buffer.
// On error, dst is returned with its original length, but bytes in dst[len(dst):cap(dst)] may be overwritten.
// Encoding is allocation-free when v is a pointer, dst has enough capacity and the codec is already cached.
func (c *frozenConfig) AppendMarshal(dst []byte, schema Schema, v any) ([]byte, error) {
	writer := c.borrowWriter()
	pooled := writer.buf
	writer.buf = dst

	writer.WriteVal(schema, v)
	out, err := writer.buf, writer.Error

	writer.buf = pooled
	c.returnWriter(writer)

	if err != nil {
		return dst, err
	}
	return out, nil
}
