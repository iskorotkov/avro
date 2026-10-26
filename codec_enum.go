package avro

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"unsafe"
)

func createDecoderOfEnum(schema *EnumSchema, typ reflect.Type) ValDecoder {
	switch {
	case typ.Kind() == reflect.String:
		return &enumCodec{enum: schema}
	case typ.Implements(textUnmarshalerType):
		return &enumTextMarshalerCodec{text: newTextValue(typ), enum: schema}
	case reflect.PointerTo(typ).Implements(textUnmarshalerType):
		return &enumTextMarshalerCodec{text: newAddrTextValue(typ), enum: schema}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfEnum(schema *EnumSchema, typ reflect.Type) ValEncoder {
	switch {
	case typ.Kind() == reflect.String:
		return &enumCodec{enum: schema}
	case typ.Implements(textAppenderType):
		return &enumTextAppenderCodec{text: newTextValue(typ), enum: schema}
	case reflect.PointerTo(typ).Implements(textAppenderType):
		return &enumTextAppenderCodec{text: newAddrTextValue(typ), enum: schema}
	case typ.Implements(textMarshalerType):
		return &enumTextMarshalerCodec{text: newTextValue(typ), enum: schema}
	case reflect.PointerTo(typ).Implements(textMarshalerType):
		return &enumTextMarshalerCodec{text: newAddrTextValue(typ), enum: schema}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type enumCodec struct {
	enum *EnumSchema
}

func (c *enumCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := int(r.ReadInt())

	symbol, ok := c.enum.Symbol(i)
	if !ok {
		r.ReportError("decode enum symbol", "unknown enum symbol")
		return
	}

	*(*string)(ptr) = symbol
}

func (c *enumCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	str := *(*string)(ptr)
	if i := slices.Index(c.enum.symbols, str); i >= 0 {
		w.WriteInt(int32(i))
		return
	}

	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", str)
}

type enumTextMarshalerCodec struct {
	text textValue
	enum *EnumSchema
}

func (c *enumTextMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := int(r.ReadInt())

	symbol, ok := c.enum.Symbol(i)
	if !ok {
		r.ReportError("decode enum symbol", "unknown enum symbol")
		return
	}

	unmarshaler, ok := c.text.valueOrNew(ptr).(encoding.TextUnmarshaler)
	if !ok {
		r.ReportError("decode enum text unmarshaler", "can not unmarshal into nil")
		return
	}
	if err := unmarshaler.UnmarshalText([]byte(symbol)); err != nil {
		r.ReportError("decode enum text unmarshaler", err.Error())
	}
}

func (c *enumTextMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.text.value(ptr)
	if obj == nil {
		w.Error = errors.New("encoding nil enum text marshaler")
		return
	}
	marshaler := obj.(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}

	for i, sym := range c.enum.symbols {
		if string(b) != sym {
			continue
		}

		w.WriteInt(int32(i))
		return
	}

	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", string(b))
}

type enumTextAppenderCodec struct {
	text textValue
	enum *EnumSchema
}

func (c *enumTextAppenderCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.text.value(ptr)
	if obj == nil {
		w.Error = errors.New("encoding nil enum text appender")
		return
	}
	// AppendText must derive its result from the passed-in buffer, not alias internal state, or w.scratch reuse will corrupt later calls.
	appender := obj.(encoding.TextAppender)
	b, err := appender.AppendText(w.scratch[:0])
	if err != nil {
		w.Error = err
		return
	}

	for i, sym := range c.enum.symbols {
		if string(b) != sym {
			continue
		}

		w.scratch = b
		w.WriteInt(int32(i))
		return
	}

	w.scratch = b
	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", string(b))
}
