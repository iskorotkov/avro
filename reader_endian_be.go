//go:build !(386 || amd64 || amd64p32 || arm || arm64 || loong64 || mipsle || mips64le || mips64p32le || ppc64le || riscv || riscv64 || wasm)

package avro

import (
	"encoding/binary"
	"math"
)

// Mirrors reader_endian_le.go: the spec mandates little-endian floats, so big-endian and unknown hosts must decode explicitly; kept as whole-function copies because shared helpers would push ReadFloat/ReadDouble past the inlining budget.

// ReadFloat reads a Float from the Reader.
func (r *Reader) ReadFloat() float32 {
	r.head += 4
	if r.head <= r.tail {
		return math.Float32frombits(binary.LittleEndian.Uint32(r.buf[r.head-4:]))
	}
	return r.readFloatSlow()
}

//go:noinline
func (r *Reader) readFloatSlow() float32 {
	r.head -= 4
	var buf [4]byte
	r.Read(buf[:])
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[:]))
}

// ReadDouble reads a Double from the Reader.
func (r *Reader) ReadDouble() float64 {
	r.head += 8
	if r.head <= r.tail {
		return math.Float64frombits(binary.LittleEndian.Uint64(r.buf[r.head-8:]))
	}
	return r.readDoubleSlow()
}

//go:noinline
func (r *Reader) readDoubleSlow() float64 {
	r.head -= 8
	var buf [8]byte
	r.Read(buf[:])
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[:]))
}
