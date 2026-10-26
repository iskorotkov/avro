package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
	"github.com/modern-go/reflect2"
)

func createDecoderOfArray(d *decoderContext, schema *ArraySchema, typ reflect2.Type) ValDecoder {
	if typ.Kind() != reflect.Slice {
		return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	if dec, ok := newScalarArrayDecoder(schema, typ.Type1()); ok {
		return dec
	}

	return decoderOfArray(d, schema, typ.Type1())
}

func createEncoderOfArray(e *encoderContext, schema *ArraySchema, typ reflect2.Type) ValEncoder {
	if typ.Kind() != reflect.Slice {
		return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	if enc, ok := newScalarArrayEncoder(e, schema, typ.Type1()); ok {
		return enc
	}

	return encoderOfArray(e, schema, typ.Type1())
}

func decoderOfArray(d *decoderContext, arr *ArraySchema, typ reflect.Type) ValDecoder {
	decoder := decoderOfType(d, arr.Items(), reflect2.Type2(typ.Elem()))

	return &arrayDecoder{typ: typ, elemSize: typ.Elem().Size(), decoder: decoder}
}

type arrayDecoder struct {
	typ      reflect.Type
	elemSize uintptr
	decoder  ValDecoder
}

func (d *arrayDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	var size int

	unsafex.SetEmptySlice(ptr)

	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		start := size
		if l > r.cfg.getMaxSliceAllocSize()-size {
			r.ReportError("decode array", "size is greater than `Config.MaxSliceAllocSize`")
			return
		}
		size += l

		unsafex.GrowSlice(d.typ, ptr, size)

		for i := start; i < size; i++ {
			elemPtr := unsafex.SliceIndex(ptr, i, d.elemSize)
			d.decoder.Decode(elemPtr, r)
			if r.Error != nil {
				r.Error = fmt.Errorf("reading %s: %w", d.typ, r.Error)
				return
			}
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", d.typ, r.Error)
	}
}

func encoderOfArray(e *encoderContext, arr *ArraySchema, typ reflect.Type) ValEncoder {
	encoder := encoderOfType(e, arr.Items(), reflect2.Type2(typ.Elem()))

	return &arrayEncoder{
		blockLength: e.cfg.getBlockLength(),
		typ:         typ,
		elemSize:    typ.Elem().Size(),
		encoder:     encoder,
	}
}

type arrayEncoder struct {
	blockLength int
	typ         reflect.Type
	elemSize    uintptr
	encoder     ValEncoder
}

func (e *arrayEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	blockLength := e.blockLength
	length := unsafex.SliceLen(ptr)

	for i := 0; i < length; i += blockLength {
		w.WriteBlockCB(func(w *Writer) int64 {
			count := int64(0)
			for j := i; j < i+blockLength && j < length; j++ {
				elemPtr := unsafex.SliceIndex(ptr, j, e.elemSize)
				e.encoder.Encode(elemPtr, w)
				if w.Error != nil && !errors.Is(w.Error, io.EOF) {
					w.Error = fmt.Errorf("%s: %w", e.typ, w.Error)
					return count
				}
				count++
			}

			return count
		})
	}

	w.WriteBlockHeader(0, 0)

	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = fmt.Errorf("%v: %w", e.typ, w.Error)
	}
}
