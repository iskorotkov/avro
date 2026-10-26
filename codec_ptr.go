package avro

import (
	"errors"
	"reflect"
	"unsafe"
)

func decoderOfPtr(d *decoderContext, schema Schema, typ reflect.Type) ValDecoder {
	elemType := typ.Elem()

	decoder := decoderOfType(d, schema, elemType)

	return &dereferenceDecoder{typ: elemType, decoder: decoder}
}

type dereferenceDecoder struct {
	typ     reflect.Type
	decoder ValDecoder
}

func (d *dereferenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if *(*unsafe.Pointer)(ptr) == nil {
		// Create new instance
		newPtr := reflect.New(d.typ).UnsafePointer()
		d.decoder.Decode(newPtr, r)
		*(*unsafe.Pointer)(ptr) = newPtr
		return
	}

	// Reuse existing instance
	d.decoder.Decode(*(*unsafe.Pointer)(ptr), r)
}

func encoderOfPtr(e *encoderContext, schema Schema, typ reflect.Type) ValEncoder {
	enc := encoderOfType(e, schema, typ.Elem())

	return &dereferenceEncoder{encoder: enc}
}

type dereferenceEncoder struct {
	encoder ValEncoder
}

func (d *dereferenceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if *(*unsafe.Pointer)(ptr) == nil {
		w.Error = errors.New("avro: cannot encode nil pointer")
		return
	}

	d.encoder.Encode(*(*unsafe.Pointer)(ptr), w)
}

type referenceDecoder struct {
	decoder ValDecoder
}

func (decoder *referenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	decoder.decoder.Decode(unsafe.Pointer(&ptr), r)
}
