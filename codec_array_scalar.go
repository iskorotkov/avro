package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func newScalarArrayDecoder(schema *ArraySchema, sliceType *reflect2.UnsafeSliceType) (ValDecoder, bool) {
	prim, ok := schema.Items().(*PrimitiveSchema)
	if !ok || prim.encodedType != "" || prim.logical != nil {
		return nil, false
	}

	elemKind := sliceType.Elem().Kind()
	switch prim.Type() {
	case Int:
		switch elemKind {
		case reflect.Int:
			return &scalarArrayDecoder[int]{sliceType, fillInts[int]}, true
		case reflect.Int8:
			return &scalarArrayDecoder[int8]{sliceType, fillInts[int8]}, true
		case reflect.Int16:
			return &scalarArrayDecoder[int16]{sliceType, fillInts[int16]}, true
		case reflect.Int32:
			return &scalarArrayDecoder[int32]{sliceType, fillInts[int32]}, true
		case reflect.Uint:
			return &scalarArrayDecoder[uint]{sliceType, fillInts[uint]}, true
		case reflect.Uint8:
			return &scalarArrayDecoder[uint8]{sliceType, fillInts[uint8]}, true
		case reflect.Uint16:
			return &scalarArrayDecoder[uint16]{sliceType, fillInts[uint16]}, true
		}
	case Long:
		switch elemKind {
		case reflect.Int:
			if strconv.IntSize == 64 {
				return &scalarArrayDecoder[int]{sliceType, fillLongs[int]}, true
			}
		case reflect.Int32:
			return &scalarArrayDecoder[int32]{sliceType, fillLongs[int32]}, true
		case reflect.Int64:
			return &scalarArrayDecoder[int64]{sliceType, fillLongs[int64]}, true
		case reflect.Uint32:
			return &scalarArrayDecoder[uint32]{sliceType, fillLongs[uint32]}, true
		}
	case Float:
		if elemKind == reflect.Float32 {
			return &scalarArrayDecoder[float32]{sliceType, fillFloat32s}, true
		}
	case Double:
		if elemKind == reflect.Float64 {
			return &scalarArrayDecoder[float64]{sliceType, fillFloat64s}, true
		}
	case Boolean:
		if elemKind == reflect.Bool {
			return &scalarArrayDecoder[bool]{sliceType, fillBools}, true
		}
	case String:
		if elemKind == reflect.String {
			return &scalarArrayDecoder[string]{sliceType, fillStrings}, true
		}
	}

	return nil, false
}

type scalarArrayDecoder[T any] struct {
	sliceType *reflect2.UnsafeSliceType
	fill      func(data []T, r *Reader)
}

func (d *scalarArrayDecoder[T]) Decode(ptr unsafe.Pointer, r *Reader) {
	sliceType := d.sliceType

	if sliceType.UnsafeIsNil(ptr) {
		sliceType.UnsafeSet(ptr, sliceType.UnsafeMakeSlice(0, 0))
	}

	var size int
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if l > r.cfg.getMaxSliceAllocSize()-size {
			r.ReportError("decode array", "size is greater than `Config.MaxSliceAllocSize`")
			return
		}

		newSize := size + l
		sliceType.UnsafeGrow(ptr, newSize)

		base := sliceType.UnsafeGetIndex(ptr, size)
		data := unsafe.Slice((*T)(base), l)

		d.fill(data, r)
		if r.Error != nil {
			r.Error = fmt.Errorf("reading %s: %w", sliceType.String(), r.Error)
			return
		}

		size = newSize
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", sliceType, r.Error)
	}
}

// Pre-allocated sentinel errors so the decode closures stay inlineable
// (formatting via r.ReportError pushes the closure over Go's inline budget).
var (
	errReadIntOverflow  = errors.New("avro: ReadInt: int overflow")
	errReadLongOverflow = errors.New("avro: ReadLong: int overflow")
)

// continueVarint32 decodes a multi-byte zigzag varint after the caller has
// peeked the first byte (with high bit set). Inlines at cost 48.
func continueVarint32(tail []byte, b0 byte) (uint32, int, bool) {
	v := uint32(b0 & 0x7f)
	s := uint8(7)
	for k, b := range tail {
		v |= uint32(b&0x7f) << s
		if b&0x80 == 0 {
			return v, k + 2, true
		}
		s += 7
	}
	return 0, 0, false
}

// continueVarint64 is the maxLongBufSize counterpart of continueVarint32.
func continueVarint64(tail []byte, b0 byte) (uint64, int, bool) {
	v := uint64(b0 & 0x7f)
	s := uint8(7)
	for k, b := range tail {
		v |= uint64(b&0x7f) << s
		if b&0x80 == 0 {
			return v, k + 2, true
		}
		s += 7
	}
	return 0, 0, false
}

// fillInts decodes len(data) zigzag-encoded int varints from r into data.
// 2× unroll with inline 1-byte peek (dominant case); multi-byte fallback
// calls continueVarint32, which inlines. The decode closure sets r.Error
// to a sentinel on overflow so callers only check r.Error in the loop.
// 4× was tried and lost ~4% — closure captures `head`, so a longer chain
// of sequential decode() calls serializes through head and kills ILP.
func fillInts[T smallInt](data []T, r *Reader) {
	if r.Error != nil {
		return
	}
	head := r.head
	n := len(data)
	i := 0
	decode := func() uint32 {
		b := r.buf[head]
		if b < 0x80 {
			head++
			return uint32(b)
		}
		v, adv, ok := continueVarint32(r.buf[head+1:head+maxIntBufSize], b)
		if !ok {
			r.Error = errReadIntOverflow
			return 0
		}
		head += adv
		return v
	}
	for r.Error == nil && i+2 <= n && r.tail-head >= 2*maxIntBufSize {
		v0, v1 := decode(), decode()
		data[i] = T(int32((v0 >> 1) ^ -(v0 & 1)))
		data[i+1] = T(int32((v1 >> 1) ^ -(v1 & 1)))
		i += 2
	}
	for r.Error == nil && i < n && r.tail-head >= maxIntBufSize {
		v := decode()
		data[i] = T(int32((v >> 1) ^ -(v & 1)))
		i++
	}
	r.head = head
	for ; r.Error == nil && i < n; i++ {
		data[i] = T(r.ReadInt())
	}
}

// fillLongs is the maxLongBufSize counterpart of fillInts.
func fillLongs[T largeInt](data []T, r *Reader) {
	if r.Error != nil {
		return
	}
	head := r.head
	n := len(data)
	i := 0
	decode := func() uint64 {
		b := r.buf[head]
		if b < 0x80 {
			head++
			return uint64(b)
		}
		v, adv, ok := continueVarint64(r.buf[head+1:head+maxLongBufSize], b)
		if !ok {
			r.Error = errReadLongOverflow
			return 0
		}
		head += adv
		return v
	}
	for r.Error == nil && i+2 <= n && r.tail-head >= 2*maxLongBufSize {
		v0, v1 := decode(), decode()
		data[i] = T(int64((v0 >> 1) ^ -(v0 & 1)))
		data[i+1] = T(int64((v1 >> 1) ^ -(v1 & 1)))
		i += 2
	}
	for r.Error == nil && i < n && r.tail-head >= maxLongBufSize {
		v := decode()
		data[i] = T(int64((v >> 1) ^ -(v & 1)))
		i++
	}
	r.head = head
	for ; r.Error == nil && i < n; i++ {
		data[i] = T(r.ReadLong())
	}
}

func fillFloat32s(data []float32, r *Reader) {
	for i := range data {
		data[i] = r.ReadFloat()
		if r.Error != nil {
			return
		}
	}
}

func fillFloat64s(data []float64, r *Reader) {
	for i := range data {
		data[i] = r.ReadDouble()
		if r.Error != nil {
			return
		}
	}
}

func fillBools(data []bool, r *Reader) {
	for i := range data {
		data[i] = r.ReadBool()
		if r.Error != nil {
			return
		}
	}
}

func fillStrings(data []string, r *Reader) {
	for i := range data {
		data[i] = r.ReadString()
		if r.Error != nil {
			return
		}
	}
}
