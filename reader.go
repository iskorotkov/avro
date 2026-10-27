package avro

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/bytesx"
)

const (
	maxIntBufSize  = 5
	maxLongBufSize = 10
)

// ReaderFunc is a function used to customize the Reader.
type ReaderFunc func(r *Reader)

// WithReaderConfig specifies the configuration to use with a reader.
func WithReaderConfig(cfg API) ReaderFunc {
	return func(r *Reader) {
		r.cfg = cfg.(*frozenConfig)
	}
}

// Reader is an Avro specific io.Reader.
type Reader struct {
	cfg    *frozenConfig
	reader io.Reader
	sized  lengther
	slab   []byte
	buf    []byte
	head   int
	tail   int
	limits readerLimits
	Error  error
}

// NewReader creates a new Reader; bufSize is ignored and the buffer size comes from `Config.ReadBufSize`.
func NewReader(r io.Reader, bufSize int, opts ...ReaderFunc) *Reader {
	reader := &Reader{
		cfg:    DefaultConfig.(*frozenConfig),
		reader: r,
	}
	for _, opt := range opts {
		opt(reader)
	}
	reader.sized = sizedReader(r)
	reader.buf = make([]byte, max(0, reader.cfg.getReadBufSize()))
	return reader
}

// sizedReader returns r when it is a known in-memory reader whose Len reports the unread bytes.
func sizedReader(r io.Reader) lengther {
	switch rd := r.(type) {
	case *bytes.Reader:
		return rd
	case *bytes.Buffer:
		return rd
	case *strings.Reader:
		return rd
	case *bytesx.ResetReader:
		return rd
	default:
		return nil
	}
}

// Reset resets a Reader with a new byte array attached.
func (r *Reader) Reset(b []byte) *Reader {
	r.reader = nil
	r.sized = nil
	r.limits = readerLimits{}
	r.buf = b
	r.head = 0
	r.tail = len(b)
	return r
}

// ReportError record an error in iterator instance with current position.
func (r *Reader) ReportError(operation, msg string) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		return
	}

	r.Error = fmt.Errorf("avro: %s: %s", operation, msg)
}

func (r *Reader) loadMore() bool {
	if r.reader == nil {
		if r.Error == nil {
			r.head = r.tail
			r.Error = io.EOF
		}
		return false
	}

	for {
		n, err := r.reader.Read(r.buf)
		if n == 0 {
			if err != nil {
				if r.Error == nil {
					r.Error = err
				}
				return false
			}
			continue
		}

		r.head = 0
		r.tail = n
		return true
	}
}

func (r *Reader) readByte() byte {
	if r.head != r.tail {
		r.head++
		return r.buf[r.head-1]
	}

	return r.readByteSlow()
}

//go:noinline
func (r *Reader) readByteSlow() byte {
	if !r.loadMore() {
		r.Error = io.ErrUnexpectedEOF
		return 0
	}

	b := r.buf[r.head]
	r.head++

	return b
}

// Peek returns the next byte in the buffer.
// The Reader Error will be io.EOF if no next byte exists.
func (r *Reader) Peek() byte {
	if r.head == r.tail {
		if !r.loadMore() {
			return 0
		}
	}
	return r.buf[r.head]
}

// Read reads data into the given bytes.
func (r *Reader) Read(b []byte) {
	size := len(b)
	read := 0

	for read < size {
		if r.head == r.tail {
			if !r.loadMore() {
				r.Error = io.ErrUnexpectedEOF
				return
			}
		}

		n := copy(b[read:], r.buf[r.head:r.tail])
		r.head += n
		read += n
	}
}

// ReadBool reads a Bool from the Reader.
func (r *Reader) ReadBool() bool {
	b := r.readByte()

	if b != 0 && b != 1 {
		r.ReportError("ReadBool", "invalid bool")
	}
	return b == 1
}

// ReadInt reads an Int from the Reader.
//
//nolint:dupl
func (r *Reader) ReadInt() int32 {
	if r.Error != nil {
		return 0
	}

	// Fast path: enough bytes in buffer for maximum varint size.
	if r.tail-r.head >= maxIntBufSize {
		var v uint32
		var s uint8
		for i, b := range r.buf[r.head : r.head+maxIntBufSize] {
			v |= uint32(b&0x7f) << s
			if b&0x80 == 0 {
				r.head += i + 1
				return int32((v >> 1) ^ -(v & 1))
			}
			s += 7
		}
		r.ReportError("ReadInt", "int overflow")
		return 0
	}

	// Slow path: not enough bytes in buffer, may need to load more.
	var (
		n int
		v uint32
		s uint8
	)

	for {
		tail := r.tail
		if r.tail-r.head+n > maxIntBufSize {
			tail = r.head + maxIntBufSize - n
		}

		// Consume what it is in the buffer.
		var i int
		for _, b := range r.buf[r.head:tail] {
			v |= uint32(b&0x7f) << s
			if b&0x80 == 0 {
				r.head += i + 1
				return int32((v >> 1) ^ -(v & 1))
			}
			s += 7
			i++
		}
		if n >= maxIntBufSize {
			r.ReportError("ReadInt", "int overflow")
			return 0
		}
		r.head += i
		n += i

		// We ran out of buffer and are not at the end of the int,
		// Read more into the buffer.
		if !r.loadMore() {
			r.Error = fmt.Errorf("reading int: %w", r.Error)
			return 0
		}
	}
}

// ReadLong reads a Long from the Reader.
//
//nolint:dupl
func (r *Reader) ReadLong() int64 {
	if r.Error != nil {
		return 0
	}

	// Fast path: enough bytes in buffer for maximum varint size.
	if r.tail-r.head >= maxLongBufSize {
		var v uint64
		var s uint8
		for i, b := range r.buf[r.head : r.head+maxLongBufSize] {
			v |= uint64(b&0x7f) << s
			if b&0x80 == 0 {
				r.head += i + 1
				return int64((v >> 1) ^ -(v & 1))
			}
			s += 7
		}
		r.ReportError("ReadLong", "int overflow")
		return 0
	}

	// Slow path: not enough bytes in buffer, may need to load more.
	var (
		n int
		v uint64
		s uint8
	)

	for {
		tail := r.tail
		if r.tail-r.head+n > maxLongBufSize {
			tail = r.head + maxLongBufSize - n
		}

		// Consume what it is in the buffer.
		var i int
		for _, b := range r.buf[r.head:tail] {
			v |= uint64(b&0x7f) << s
			if b&0x80 == 0 {
				r.head += i + 1
				return int64((v >> 1) ^ -(v & 1))
			}
			s += 7
			i++
		}
		if n >= maxLongBufSize {
			r.ReportError("ReadLong", "int overflow")
			return 0
		}
		r.head += i
		n += i

		// We ran out of buffer and are not at the end of the long,
		// Read more into the buffer.
		if !r.loadMore() {
			r.Error = fmt.Errorf("reading long: %w", r.Error)
			return 0
		}
	}
}

// ReadBytes reads Bytes from the Reader.
func (r *Reader) ReadBytes() []byte {
	return r.readBytes("ReadBytes", "bytes")
}

// ReadString reads a String from the Reader.
func (r *Reader) ReadString() string {
	b := r.readBytes("ReadString", "string")
	if len(b) == 0 {
		return ""
	}

	return *(*string)(unsafe.Pointer(&b))
}

func (r *Reader) readBytes(fnName, op string) []byte {
	size64 := r.ReadLong()
	if size64 < 0 {
		r.ReportError(fnName, "invalid "+op+" length")
		return nil
	}
	if size64 == 0 {
		return []byte{}
	}
	if maxSize := r.cfg.getMaxByteSliceSize(); size64 > int64(maxSize) {
		r.reportErr(fnName, ErrMaxByteSliceSize)
		return nil
	}

	size := int(size64)

	// The bytes are entirely in the buffer and of a reasonable size.
	// Use the byte slab.
	if r.head+size <= r.tail && size <= r.cfg.getSlabSize() {
		if cap(r.slab) < size {
			r.slab = make([]byte, r.cfg.getSlabSize())
		}

		_ = r.slab[size-1] // Bounds check hint to compiler.

		dst := r.slab[:size]
		r.slab = r.slab[size:]
		copy(dst, r.buf[r.head:r.head+size])
		r.head += size
		return dst
	}

	if rem := r.remainingInput(); rem >= 0 && size64 > int64(rem) {
		r.reportTruncated(fnName, op+" length exceeds remaining input")
		return nil
	}

	buf := make([]byte, size)
	r.Read(buf)
	if r.Error != nil {
		return nil
	}
	return buf
}

// ReadBlockHeader reads a Block Header from the Reader.
func (r *Reader) ReadBlockHeader() (int, int) {
	length64 := r.ReadLong()
	if length64 > math.MaxInt {
		r.ReportError("read block header", "block length is too big")
		return 0, 0
	}
	// check for too small value on 32-bit architecture and for math.MinInt on both 32- and 64-bit which cannot be negated
	if length64 <= math.MinInt {
		r.ReportError("read block header", "block length is too small")
		return 0, 0
	}

	length := int(length64)
	if length >= 0 {
		return length, 0
	}

	size64 := r.ReadLong()
	if size64 > math.MaxInt {
		r.ReportError("read block header", "skip size is too big")
		return 0, 0
	}
	if size64 < 0 {
		r.ReportError("read block header", "skip size is too small")
		return 0, 0
	}
	size := int(size64)

	return -length, size
}
