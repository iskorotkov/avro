package avro

// readAliased returns the next size bytes of the input without copying them.
func (r *Reader) readAliased(size int) []byte {
	b := r.buf[r.head : r.head+size : r.head+size]
	r.head += size
	return b
}
