package avro

import (
	"encoding"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	textMarshalerType   = reflect2.TypeOfPtr((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerType = reflect2.TypeOfPtr((*encoding.TextUnmarshaler)(nil)).Elem()
	// textAppenderType is preferred over textMarshalerType at every dispatch site; encoding contract requires AppendText output to match MarshalText.
	textAppenderType = reflect2.TypeOfPtr((*encoding.TextAppender)(nil)).Elem()
)

func createDecoderOfMarshaler(schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Implements(textUnmarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{typ}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(textUnmarshalerType) && schema.Type() == String {
		return &referenceDecoder{
			&textMarshalerCodec{ptrType},
		}
	}
	return nil
}

func createEncoderOfMarshaler(schema Schema, typ reflect2.Type) ValEncoder {
	if schema.Type() != String {
		return nil
	}
	if typ.Implements(textAppenderType) {
		return &textAppenderCodec{typ: typ}
	}
	if typ.Implements(textMarshalerType) {
		return &textMarshalerCodec{typ: typ}
	}
	return nil
}

type textMarshalerCodec struct {
	typ reflect2.Type
}

func (c textMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(encoding.TextUnmarshaler)
	b := r.ReadBytes()
	err := unmarshaler.UnmarshalText(b)
	if err != nil {
		r.ReportError("textMarshalerCodec", err.Error())
	}
}

func (c textMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.WriteBytes(nil)
		return
	}
	marshaler := (obj).(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}

type textAppenderCodec struct {
	typ reflect2.Type
}

func (c textAppenderCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.WriteBytes(nil)
		return
	}
	// AppendText must derive its result from the passed-in buffer, not alias internal state, or w.scratch reuse will corrupt later calls.
	appender := (obj).(encoding.TextAppender)
	b, err := appender.AppendText(w.scratch[:0])
	if err != nil {
		w.Error = err
		return
	}
	w.scratch = b
	w.WriteBytes(b)
}
