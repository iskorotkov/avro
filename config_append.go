package avro

// AppendMarshal appends the Avro encoding of v to dst and returns the extended buffer.
// On error, dst is returned unchanged in length.
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
