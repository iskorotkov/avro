package avro

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
)

func createDecoderOfMap(d *decoderContext, schema *MapSchema, typ reflect.Type) ValDecoder {
	if typ.Kind() == reflect.Map {
		keyType := typ.Key()
		switch {
		case keyType.Kind() == reflect.String:
			return decoderOfMap(d, schema, typ)
		case keyType.Implements(textUnmarshalerType):
			return decoderOfMapUnmarshaler(d, schema, typ)
		}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfMap(e *encoderContext, schema *MapSchema, typ reflect.Type) ValEncoder {
	if typ.Kind() == reflect.Map {
		keyType := typ.Key()
		switch {
		case keyType.Kind() == reflect.String:
			return encoderOfMap(e, schema, typ)
		case keyType.Implements(textAppenderType):
			return encoderOfMapKeys(e, schema, typ, textAppenderKey{boxer: unsafex.NewBoxer(keyType)})
		case keyType.Implements(textMarshalerType):
			return encoderOfMapKeys(e, schema, typ, textMarshalerKey{boxer: unsafex.NewBoxer(keyType)})
		}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

// nextMapBlock reads the next block length, returning -1 when the block exceeds Config.MaxMapAllocSize.
func nextMapBlock(r *Reader, size *int) int {
	l, _ := r.ReadBlockHeader()
	if l == 0 {
		return 0
	}

	if l > r.cfg.getMaxMapAllocSize()-*size {
		r.ReportError("decode map", "size is greater than `Config.MaxMapAllocSize`")
		return -1
	}
	*size += l

	return l
}

func wrapMapDecodeError(r *Reader, typ reflect.Type) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", typ, r.Error)
	}
}

var anyMapType = reflect.TypeFor[map[string]any]()

// makeMapIfNil stores a new map with room for n elements at ptr when the map there is nil.
func makeMapIfNil(typ reflect.Type, ptr unsafe.Pointer, n int) {
	if *(*unsafe.Pointer)(ptr) == nil {
		unsafex.MakeMap(typ, ptr, n)
	}
}

// decodeMapEntry decodes a value with dec and stores it under key in the non-nil map of type typ at ptr.
func decodeMapEntry(typ reflect.Type, ptr unsafe.Pointer, key string, dec ValDecoder, r *Reader) {
	if typ == anyMapType {
		var elem any
		dec.Decode(noescape(unsafe.Pointer(&elem)), r)
		(*(*map[string]any)(ptr))[key] = elem
		return
	}

	elem := reflect.New(typ.Elem())
	dec.Decode(elem.UnsafePointer(), r)
	unsafex.MapOf(typ, ptr).SetMapIndex(reflect.ValueOf(key).Convert(typ.Key()), elem.Elem())
}

func decoderOfMap(d *decoderContext, m *MapSchema, typ reflect.Type) ValDecoder {
	decoder := decoderOfType(d, m.Values(), typ.Elem())

	switch typ {
	case reflect.TypeFor[map[string]string]():
		return &nativeMapDecoder[string]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string][]byte]():
		return &nativeMapDecoder[[]byte]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]int]():
		return &nativeMapDecoder[int]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]int32]():
		return &nativeMapDecoder[int32]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]int64]():
		return &nativeMapDecoder[int64]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]float32]():
		return &nativeMapDecoder[float32]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]float64]():
		return &nativeMapDecoder[float64]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]bool]():
		return &nativeMapDecoder[bool]{typ: typ, decoder: decoder}
	case reflect.TypeFor[map[string]any]():
		return &nativeMapDecoder[any]{typ: typ, decoder: decoder}
	default:
		return &mapDecoder{typ: typ, decoder: decoder}
	}
}

// nativeMapDecoder decodes into maps whose exact type is map[string]V.
type nativeMapDecoder[V any] struct {
	typ     reflect.Type
	decoder ValDecoder
}

func (d *nativeMapDecoder[V]) Decode(ptr unsafe.Pointer, r *Reader) {
	m := (*map[string]V)(ptr)
	existing := len(*m) > 0
	var size int

	for {
		l := nextMapBlock(r, &size)
		if l < 0 {
			return
		}
		if l == 0 {
			break
		}

		if *m == nil {
			*m = make(map[string]V, l)
		}

		for range l {
			key := r.ReadString()

			var elem V
			if existing {
				elem = (*m)[key]
			}

			d.decoder.Decode(noescape(unsafe.Pointer(&elem)), r)
			if r.Error != nil {
				r.Error = fmt.Errorf("reading %s: %w", d.typ, r.Error)
				return
			}

			(*m)[key] = elem
		}
	}

	if *m == nil {
		*m = map[string]V{}
	}

	wrapMapDecodeError(r, d.typ)
}

// mapDecoder decodes into maps with string-kinded keys through reflection.
type mapDecoder struct {
	typ     reflect.Type
	decoder ValDecoder
}

func (d *mapDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	m := unsafex.MapOf(d.typ, ptr)
	existing := m.Len() > 0
	key := reflect.New(d.typ.Key()).Elem()
	elemPtr := reflect.New(d.typ.Elem())
	elem := elemPtr.Elem()
	var size int

	for {
		l := nextMapBlock(r, &size)
		if l < 0 {
			return
		}
		if l == 0 {
			break
		}

		if m.IsNil() {
			unsafex.MakeMap(d.typ, ptr, l)
		}

		for range l {
			key.SetString(r.ReadString())

			elem.SetZero()
			if existing {
				if v := m.MapIndex(key); v.IsValid() {
					elem.Set(v)
				}
			}

			d.decoder.Decode(elemPtr.UnsafePointer(), r)
			if r.Error != nil {
				r.Error = fmt.Errorf("reading %s: %w", d.typ, r.Error)
				return
			}

			m.SetMapIndex(key, elem)
		}
	}

	if m.IsNil() {
		unsafex.MakeMap(d.typ, ptr, 0)
	}

	wrapMapDecodeError(r, d.typ)
}

func decoderOfMapUnmarshaler(d *decoderContext, m *MapSchema, typ reflect.Type) ValDecoder {
	decoder := decoderOfType(d, m.Values(), typ.Elem())

	return &mapDecoderUnmarshaler{
		typ:     typ,
		keyBox:  unsafex.NewBoxer(typ.Key()),
		decoder: decoder,
	}
}

// mapDecoderUnmarshaler decodes into maps with encoding.TextUnmarshaler keys.
type mapDecoderUnmarshaler struct {
	typ     reflect.Type
	keyBox  unsafex.Boxer
	decoder ValDecoder
}

func (d *mapDecoderUnmarshaler) Decode(ptr unsafe.Pointer, r *Reader) {
	m := unsafex.MapOf(d.typ, ptr)
	keyType := d.typ.Key()
	keyPtr := reflect.New(keyType)
	key := keyPtr.Elem()
	elemPtr := reflect.New(d.typ.Elem())
	elem := elemPtr.Elem()
	var size int

	for {
		l := nextMapBlock(r, &size)
		if l < 0 {
			return
		}
		if l == 0 {
			break
		}

		if m.IsNil() {
			unsafex.MakeMap(d.typ, ptr, l)
		}

		for range l {
			if keyType.Kind() == reflect.Pointer {
				key.Set(reflect.New(keyType.Elem()))
			} else {
				key.SetZero()
			}

			unmarshaler := d.keyBox.Box(keyPtr.UnsafePointer()).(encoding.TextUnmarshaler)
			if err := unmarshaler.UnmarshalText(r.ReadBytes()); err != nil {
				r.ReportError("mapDecoderUnmarshaler", err.Error())
				return
			}

			elem.SetZero()
			d.decoder.Decode(elemPtr.UnsafePointer(), r)

			m.SetMapIndex(key, elem)
		}
	}

	if m.IsNil() {
		unsafex.MakeMap(d.typ, ptr, 0)
	}

	wrapMapDecodeError(r, d.typ)
}

func wrapMapEncodeError(w *Writer, typ reflect.Type) {
	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = fmt.Errorf("%v: %w", typ, w.Error)
	}
}

func encoderOfMap(e *encoderContext, m *MapSchema, typ reflect.Type) ValEncoder {
	encoder := encoderOfType(e, m.Values(), typ.Elem())
	blockLength := e.cfg.getBlockLength()

	// Maps are iterated as a map[string]V with the same memory layout as their actual type.
	elemType := typ.Elem()
	switch elemType.Kind() {
	case reflect.String:
		return &nativeMapEncoder[string]{typ: typ, blockLength: blockLength, encoder: encoder}
	case reflect.Slice:
		return &nativeMapEncoder[[]byte]{typ: typ, blockLength: blockLength, encoder: encoder}
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return &nativeMapEncoder[unsafe.Pointer]{typ: typ, blockLength: blockLength, encoder: encoder}
	case reflect.Interface:
		if elemType.NumMethod() == 0 {
			return &nativeMapEncoder[any]{typ: typ, blockLength: blockLength, encoder: encoder}
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		switch elemType.Size() {
		case 1:
			return &nativeMapEncoder[uint8]{typ: typ, blockLength: blockLength, encoder: encoder}
		case 2:
			return &nativeMapEncoder[uint16]{typ: typ, blockLength: blockLength, encoder: encoder}
		case 4:
			return &nativeMapEncoder[uint32]{typ: typ, blockLength: blockLength, encoder: encoder}
		case 8:
			return &nativeMapEncoder[uint64]{typ: typ, blockLength: blockLength, encoder: encoder}
		}
	default:
	}

	return encoderOfMapKeys(e, m, typ, stringKey{})
}

// nativeMapEncoder encodes maps with string-kinded keys whose element layout matches V.
type nativeMapEncoder[V any] struct {
	typ         reflect.Type
	blockLength int
	encoder     ValEncoder
}

func (e *nativeMapEncoder[V]) Encode(ptr unsafe.Pointer, w *Writer) {
	var block blockStart
	var n int

	for key, elem := range *(*map[string]V)(ptr) {
		if n == 0 {
			block = w.beginBlock()
		}

		w.WriteString(key)
		e.encoder.Encode(noescape(unsafe.Pointer(&elem)), w)
		if w.Error != nil {
			break
		}

		n++
		if n == e.blockLength {
			w.endBlock(block, int64(n))
			n = 0
		}
	}

	if n > 0 {
		w.endBlock(block, int64(n))
	}
	w.WriteBlockHeader(0, 0)

	wrapMapEncodeError(w, e.typ)
}

// mapKeyWriter writes a map key stored at ptr.
type mapKeyWriter interface {
	writeKey(ptr unsafe.Pointer, w *Writer)
}

type stringKey struct{}

func (stringKey) writeKey(ptr unsafe.Pointer, w *Writer) {
	w.WriteString(*(*string)(ptr))
}

type textMarshalerKey struct {
	boxer unsafex.Boxer
}

func (k textMarshalerKey) writeKey(ptr unsafe.Pointer, w *Writer) {
	obj := k.boxer.Box(ptr)
	if unsafex.IsNil(obj) {
		w.Error = errors.New("avro: mapEncoderMarshaller: encoding nil TextMarshaller")
		return
	}

	b, err := obj.(encoding.TextMarshaler).MarshalText()
	if err != nil {
		w.Error = err
		return
	}

	w.WriteBytes(b)
}

type textAppenderKey struct {
	boxer unsafex.Boxer
}

func (k textAppenderKey) writeKey(ptr unsafe.Pointer, w *Writer) {
	obj := k.boxer.Box(ptr)
	if unsafex.IsNil(obj) {
		w.Error = errors.New("avro: mapEncoderAppender: encoding nil TextAppender")
		return
	}

	// AppendText must derive its result from the passed-in buffer, not alias internal state, or w.scratch reuse will corrupt later keys.
	b, err := obj.(encoding.TextAppender).AppendText(w.scratch[:0])
	if err != nil {
		w.Error = err
		return
	}
	w.scratch = b

	w.WriteBytes(b)
}

func encoderOfMapKeys(e *encoderContext, m *MapSchema, typ reflect.Type, keys mapKeyWriter) ValEncoder {
	encoder := encoderOfType(e, m.Values(), typ.Elem())

	enc := &mapEncoder{
		typ:         typ,
		blockLength: e.cfg.getBlockLength(),
		keys:        keys,
		encoder:     encoder,
	}
	enc.iters.New = func() any {
		return newMapIterator(typ)
	}

	return enc
}

// mapIterator holds reusable iteration state for a map type.
type mapIterator struct {
	iter    reflect.MapIter
	key     reflect.Value
	elem    reflect.Value
	keyPtr  unsafe.Pointer
	elemPtr unsafe.Pointer
}

func newMapIterator(typ reflect.Type) *mapIterator {
	key := reflect.New(typ.Key())
	elem := reflect.New(typ.Elem())

	return &mapIterator{
		key:     key.Elem(),
		elem:    elem.Elem(),
		keyPtr:  key.UnsafePointer(),
		elemPtr: elem.UnsafePointer(),
	}
}

// release drops references to the iterated map so pooled iterators do not retain it.
func (it *mapIterator) release() {
	it.iter.Reset(reflect.Value{})
	it.key.SetZero()
	it.elem.SetZero()
}

// mapEncoder encodes maps of any type through reflection.
type mapEncoder struct {
	typ         reflect.Type
	blockLength int
	keys        mapKeyWriter
	encoder     ValEncoder
	iters       sync.Pool
}

func (e *mapEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	it := e.iters.Get().(*mapIterator)
	defer e.iters.Put(it)

	it.iter.Reset(unsafex.MapOf(e.typ, ptr))
	defer it.release()

	var block blockStart
	var n int

	for it.iter.Next() {
		if n == 0 {
			block = w.beginBlock()
		}

		it.key.SetIterKey(&it.iter)
		it.elem.SetIterValue(&it.iter)

		e.keys.writeKey(it.keyPtr, w)
		if w.Error != nil {
			break
		}
		e.encoder.Encode(it.elemPtr, w)
		if w.Error != nil {
			break
		}

		n++
		if n == e.blockLength {
			w.endBlock(block, int64(n))
			n = 0
		}
	}

	if n > 0 {
		w.endBlock(block, int64(n))
	}
	w.WriteBlockHeader(0, 0)

	wrapMapEncodeError(w, e.typ)
}
