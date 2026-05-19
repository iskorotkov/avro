package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func scalarArrayPrimitive(schema *ArraySchema) (*PrimitiveSchema, bool) {
	prim, ok := schema.Items().(*PrimitiveSchema)
	if !ok || prim.encodedType != "" || prim.logical != nil {
		return nil, false
	}

	return prim, true
}

func newScalarArrayDecoder(schema *ArraySchema, sliceType *reflect2.UnsafeSliceType) (ValDecoder, bool) {
	prim, ok := scalarArrayPrimitive(schema)
	if !ok {
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

func fillInts[T smallInt](data []T, r *Reader) {
	for i := range data {
		data[i] = T(r.ReadInt())
		if r.Error != nil {
			return
		}
	}
}

func fillLongs[T largeInt](data []T, r *Reader) {
	for i := range data {
		data[i] = T(r.ReadLong())
		if r.Error != nil {
			return
		}
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

func newScalarArrayEncoder(e *encoderContext, schema *ArraySchema, sliceType *reflect2.UnsafeSliceType) (ValEncoder, bool) {
	prim, ok := scalarArrayPrimitive(schema)
	if !ok {
		return nil, false
	}

	blockLength := e.cfg.getBlockLength()
	elemKind := sliceType.Elem().Kind()
	switch prim.Type() {
	case Int:
		switch elemKind {
		case reflect.Int:
			return &scalarArrayEncoder[int]{blockLength, sliceType, drainInts[int]}, true
		case reflect.Int8:
			return &scalarArrayEncoder[int8]{blockLength, sliceType, drainInts[int8]}, true
		case reflect.Int16:
			return &scalarArrayEncoder[int16]{blockLength, sliceType, drainInts[int16]}, true
		case reflect.Int32:
			return &scalarArrayEncoder[int32]{blockLength, sliceType, drainInts[int32]}, true
		case reflect.Uint:
			return &scalarArrayEncoder[uint]{blockLength, sliceType, drainInts[uint]}, true
		case reflect.Uint8:
			return &scalarArrayEncoder[uint8]{blockLength, sliceType, drainInts[uint8]}, true
		case reflect.Uint16:
			return &scalarArrayEncoder[uint16]{blockLength, sliceType, drainInts[uint16]}, true
		}
	case Long:
		switch elemKind {
		case reflect.Int:
			if strconv.IntSize == 64 {
				return &scalarArrayEncoder[int]{blockLength, sliceType, drainLongs[int]}, true
			}
		case reflect.Int32:
			return &scalarArrayEncoder[int32]{blockLength, sliceType, drainLongs[int32]}, true
		case reflect.Int64:
			return &scalarArrayEncoder[int64]{blockLength, sliceType, drainLongs[int64]}, true
		case reflect.Uint32:
			return &scalarArrayEncoder[uint32]{blockLength, sliceType, drainLongs[uint32]}, true
		}
	case Float:
		if elemKind == reflect.Float32 {
			return &scalarArrayEncoder[float32]{blockLength, sliceType, drainFloat32s}, true
		}
	case Double:
		if elemKind == reflect.Float64 {
			return &scalarArrayEncoder[float64]{blockLength, sliceType, drainFloat64s}, true
		}
	case Boolean:
		if elemKind == reflect.Bool {
			return &scalarArrayEncoder[bool]{blockLength, sliceType, drainBools}, true
		}
	case String:
		if elemKind == reflect.String {
			return &scalarArrayEncoder[string]{blockLength, sliceType, drainStrings}, true
		}
	}

	return nil, false
}

type scalarArrayEncoder[T any] struct {
	blockLength int
	sliceType   *reflect2.UnsafeSliceType
	drain       func(data []T, w *Writer)
}

func (e *scalarArrayEncoder[T]) Encode(ptr unsafe.Pointer, w *Writer) {
	sliceType := e.sliceType
	length := sliceType.UnsafeLengthOf(ptr)

	if length > 0 {
		base := sliceType.UnsafeGetIndex(ptr, 0)
		all := unsafe.Slice((*T)(base), length)
		blockLength := e.blockLength

		for i := 0; i < length; i += blockLength {
			end := min(i+blockLength, length)
			chunk := all[i:end]

			w.WriteBlockCB(func(w *Writer) int64 {
				e.drain(chunk, w)
				return int64(len(chunk))
			})

			if w.Error != nil && !errors.Is(w.Error, io.EOF) {
				w.Error = fmt.Errorf("%s: %w", sliceType.String(), w.Error)
				return
			}
		}
	}

	w.WriteBlockHeader(0, 0)

	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = fmt.Errorf("%v: %w", sliceType, w.Error)
	}
}

func drainInts[T smallInt](data []T, w *Writer) {
	buf := w.buf
	for _, v := range data {
		i := int32(v)
		z := uint64((uint32(i) << 1) ^ uint32(i>>31))
		buf = appendVarint(buf, z)
	}

	w.buf = buf
}

func drainLongs[T largeInt](data []T, w *Writer) {
	buf := w.buf
	for _, v := range data {
		i := int64(v)
		z := (uint64(i) << 1) ^ uint64(i>>63)
		buf = appendVarint(buf, z)
	}

	w.buf = buf
}

func drainFloat32s(data []float32, w *Writer) {
	buf := slices.Grow(w.buf, 4*len(data))
	for _, v := range data {
		buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(v))
	}

	w.buf = buf
}

func drainFloat64s(data []float64, w *Writer) {
	buf := slices.Grow(w.buf, 8*len(data))
	for _, v := range data {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
	}

	w.buf = buf
}

func drainBools(data []bool, w *Writer) {
	buf := slices.Grow(w.buf, len(data))
	for _, v := range data {
		b := byte(0)
		if v {
			b = 1
		}

		buf = append(buf, b)
	}

	w.buf = buf
}

func drainStrings(data []string, w *Writer) {
	for _, v := range data {
		w.WriteString(v)
	}
}
