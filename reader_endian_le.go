//go:build 386 || amd64 || amd64p32 || arm || arm64 || loong64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64 || wasm

package avro

import "unsafe"

// Mirrors reader_endian_be.go: on little-endian hosts the wire layout matches memory, so reinterpreting in place is correct; do not extract shared helpers — ReadFloat/ReadDouble sit at inline cost 79 of budget 80 and any extra expression de-inlines them.

// ReadFloat reads a Float from the Reader.
func (r *Reader) ReadFloat() float32 {
	r.head += 4
	if r.head <= r.tail {
		return *(*float32)(unsafe.Pointer(&r.buf[r.head-4]))
	}
	return r.readFloatSlow()
}

//go:noinline
func (r *Reader) readFloatSlow() float32 {
	r.head -= 4
	var buf [4]byte
	r.Read(buf[:])
	return *(*float32)(unsafe.Pointer(&buf[0]))
}

// ReadDouble reads a Double from the Reader.
func (r *Reader) ReadDouble() float64 {
	r.head += 8
	if r.head <= r.tail {
		return *(*float64)(unsafe.Pointer(&r.buf[r.head-8]))
	}
	return r.readDoubleSlow()
}

//go:noinline
func (r *Reader) readDoubleSlow() float64 {
	r.head -= 8
	var buf [8]byte
	r.Read(buf[:])
	return *(*float64)(unsafe.Pointer(&buf[0]))
}
