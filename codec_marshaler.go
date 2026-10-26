package avro

import (
	"encoding"
	"reflect"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
)

var (
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	// textAppenderType is preferred over textMarshalerType at every dispatch site; encoding contract requires AppendText output to match MarshalText.
	textAppenderType = reflect.TypeFor[encoding.TextAppender]()
)

// textValue boxes values of types implementing the text marshaling interfaces.
type textValue struct {
	typ      reflect.Type
	box      unsafex.Boxer
	nullable bool
}

func newTextValue(typ reflect.Type) textValue {
	return textValue{typ: typ, box: unsafex.NewBoxer(typ), nullable: isNullable(typ)}
}

// newAddrTextValue boxes the address of values of typ.
func newAddrTextValue(typ reflect.Type) textValue {
	return textValue{typ: reflect.PointerTo(typ), box: unsafex.NewAddrBoxer(typ)}
}

// value returns the value at ptr, or nil when it is a nil reference.
func (v textValue) value(ptr unsafe.Pointer) any {
	obj := v.box.Box(ptr)
	if v.nullable && unsafex.IsNil(obj) {
		return nil
	}
	return obj
}

// valueOrNew returns the value at ptr, allocating its target first when it is a nil pointer.
func (v textValue) valueOrNew(ptr unsafe.Pointer) any {
	if obj := v.value(ptr); obj != nil || v.typ.Kind() != reflect.Pointer {
		return obj
	}

	*(*unsafe.Pointer)(ptr) = reflect.New(v.typ.Elem()).UnsafePointer()
	return v.box.Box(ptr)
}

func isNullable(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.Slice, reflect.Interface:
		return true
	default:
		return false
	}
}

func createDecoderOfMarshaler(schema Schema, typ reflect.Type) ValDecoder {
	if typ.Implements(textUnmarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{newTextValue(typ)}
	}
	ptrType := reflect.PointerTo(typ)
	if ptrType.Implements(textUnmarshalerType) && schema.Type() == String {
		return &referenceDecoder{
			&textMarshalerCodec{newTextValue(ptrType)},
		}
	}
	return nil
}

func createEncoderOfMarshaler(schema Schema, typ reflect.Type) ValEncoder {
	if schema.Type() != String {
		return nil
	}
	if typ.Implements(textAppenderType) {
		return &textAppenderCodec{newTextValue(typ)}
	}
	if typ.Implements(textMarshalerType) {
		return &textMarshalerCodec{newTextValue(typ)}
	}
	return nil
}

type textMarshalerCodec struct {
	text textValue
}

func (c textMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	unmarshaler, ok := c.text.valueOrNew(ptr).(encoding.TextUnmarshaler)
	if !ok {
		r.ReportError("textMarshalerCodec", "can not unmarshal into nil")
		return
	}
	b := r.ReadBytes()
	err := unmarshaler.UnmarshalText(b)
	if err != nil {
		r.ReportError("textMarshalerCodec", err.Error())
	}
}

func (c textMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.text.value(ptr)
	if obj == nil {
		w.WriteBytes(nil)
		return
	}
	marshaler := obj.(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}

type textAppenderCodec struct {
	text textValue
}

func (c textAppenderCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.text.value(ptr)
	if obj == nil {
		w.WriteBytes(nil)
		return
	}
	// AppendText must derive its result from the passed-in buffer, not alias internal state, or w.scratch reuse will corrupt later calls.
	appender := obj.(encoding.TextAppender)
	b, err := appender.AppendText(w.scratch[:0])
	if err != nil {
		w.Error = err
		return
	}
	w.scratch = b
	w.WriteBytes(b)
}
