package avro

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
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
			return encoderOfMap(e, schema, typ, stringKey{})
		case keyType.Implements(textAppenderType):
			return encoderOfMap(e, schema, typ, textAppenderKey{newTextValue(keyType)})
		case keyType.Implements(textMarshalerType):
			return encoderOfMap(e, schema, typ, textMarshalerKey{newTextValue(keyType)})
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
		return &nativeMapDecoder[any]{typ: typ, decoder: decoder, merge: true}
	default:
		return &mapDecoder{typ: typ, decoder: decoder, elem: newMapElem(typ.Elem())}
	}
}

// nativeMapDecoder decodes into maps whose exact type is map[string]V.
type nativeMapDecoder[V any] struct {
	typ     reflect.Type
	decoder ValDecoder
	merge   bool
}

func (d *nativeMapDecoder[V]) Decode(ptr unsafe.Pointer, r *Reader) {
	m := (*map[string]V)(ptr)
	merge := d.merge || len(*m) > 0
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
			if merge {
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

// mapElem prepares storage for decoding map elements of a type.
type mapElem struct {
	typ   reflect.Type
	fresh bool
	merge bool
}

func newMapElem(typ reflect.Type) mapElem {
	return mapElem{typ: typ, fresh: exposesAddress(typ), merge: accumulates(typ)}
}

// scratch returns reusable storage for elements, or an invalid value when each element gets fresh storage.
func (e mapElem) scratch() reflect.Value {
	if e.fresh {
		return reflect.Value{}
	}
	return reflect.New(e.typ)
}

// next returns storage holding the current element under key, reusing scratch unless user code may retain its address.
func (e mapElem) next(scratch, m, key reflect.Value, existing bool) reflect.Value {
	if e.fresh {
		scratch = reflect.New(e.typ)
	} else {
		scratch.Elem().SetZero()
	}

	if e.merge || existing {
		if v := m.MapIndex(key); v.IsValid() {
			scratch.Elem().Set(v)
		}
	}

	return scratch
}

// mapDecoder decodes into maps with string-kinded keys through reflection.
type mapDecoder struct {
	typ     reflect.Type
	decoder ValDecoder
	elem    mapElem
}

func (d *mapDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	m := unsafex.MapOf(d.typ, ptr)
	existing := m.Len() > 0
	key := reflect.New(d.typ.Key()).Elem()
	elemPtr := d.elem.scratch()
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
			elemPtr = d.elem.next(elemPtr, m, key, existing)

			d.decoder.Decode(elemPtr.UnsafePointer(), r)
			if r.Error != nil {
				r.Error = fmt.Errorf("reading %s: %w", d.typ, r.Error)
				return
			}

			m.SetMapIndex(key, elemPtr.Elem())
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
		elem:    mapElem{typ: typ.Elem(), fresh: exposesAddress(typ.Elem())},
	}
}

// mapDecoderUnmarshaler decodes into maps with encoding.TextUnmarshaler keys.
type mapDecoderUnmarshaler struct {
	typ     reflect.Type
	keyBox  unsafex.Boxer
	decoder ValDecoder
	elem    mapElem
}

func (d *mapDecoderUnmarshaler) Decode(ptr unsafe.Pointer, r *Reader) {
	m := unsafex.MapOf(d.typ, ptr)
	keyType := d.typ.Key()
	keyPtr := reflect.New(keyType)
	key := keyPtr.Elem()
	elemPtr := d.elem.scratch()
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

			elemPtr = d.elem.next(elemPtr, m, key, false)
			d.decoder.Decode(elemPtr.UnsafePointer(), r)

			m.SetMapIndex(key, elemPtr.Elem())
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

func encoderOfMap(e *encoderContext, m *MapSchema, typ reflect.Type, keys mapKeyWriter) ValEncoder {
	base := mapEncoderBase{
		typ:         typ,
		blockLength: e.cfg.getBlockLength(),
		keys:        keys,
		encoder:     encoderOfType(e, m.Values(), typ.Elem()),
	}

	var native ValEncoder
	switch typ.Key().Kind() {
	case reflect.String:
		native = nativeMapEncoderOf[string](base)
	case reflect.Pointer:
		native = nativeMapEncoderOf[unsafe.Pointer](base)
	case reflect.Struct:
		if typ.Key() == timeType {
			native = nativeMapEncoderOf[time.Time](base)
		}
	default:
	}
	if native != nil {
		return native
	}

	return newReflectMapEncoder(base)
}

// nativeMapEncoderOf returns an encoder iterating the map as a map[K]V of the same memory layout, or nil if V has no such layout.
func nativeMapEncoderOf[K comparable](base mapEncoderBase) ValEncoder {
	elemType := base.typ.Elem()
	if exposesAddress(elemType) {
		return nil
	}

	switch elemType.Kind() {
	case reflect.String:
		return newNativeMapEncoder[K, string](base)
	case reflect.Slice:
		return newNativeMapEncoder[K, []byte](base)
	case reflect.Pointer, reflect.Map, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return newNativeMapEncoder[K, unsafe.Pointer](base)
	case reflect.Interface:
		if elemType.NumMethod() == 0 {
			return newNativeMapEncoder[K, any](base)
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		switch elemType.Size() {
		case 1:
			return newNativeMapEncoder[K, uint8](base)
		case 2:
			return newNativeMapEncoder[K, uint16](base)
		case 4:
			return newNativeMapEncoder[K, uint32](base)
		case 8:
			return newNativeMapEncoder[K, uint64](base)
		}
	default:
	}

	return nil
}

// newNativeMapEncoder returns a nativeMapEncoder, or nil when K or V differ in layout from the map's key or element.
func newNativeMapEncoder[K comparable, V any](base mapEncoderBase) ValEncoder {
	if !sameLayout(base.typ.Key(), reflect.TypeFor[K]()) || !sameLayout(base.typ.Elem(), reflect.TypeFor[V]()) {
		return nil
	}
	return &nativeMapEncoder[K, V]{base}
}

// mapEncoderBase holds what every map encoder needs to write a map.
type mapEncoderBase struct {
	typ         reflect.Type
	blockLength int
	keys        mapKeyWriter
	encoder     ValEncoder
}

// nativeMapEncoder encodes maps whose key and element layouts match K and V.
type nativeMapEncoder[K comparable, V any] struct {
	mapEncoderBase
}

func (e *nativeMapEncoder[K, V]) Encode(ptr unsafe.Pointer, w *Writer) {
	var block blockStart
	var n int

	for key, elem := range *(*map[K]V)(ptr) {
		if n == 0 {
			block = w.beginBlock()
		}

		e.keys.writeKey(noescape(unsafe.Pointer(&key)), w)
		if w.Error != nil {
			break
		}
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
	text textValue
}

func (k textMarshalerKey) writeKey(ptr unsafe.Pointer, w *Writer) {
	obj := k.text.value(ptr)
	if obj == nil {
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
	text textValue
}

func (k textAppenderKey) writeKey(ptr unsafe.Pointer, w *Writer) {
	obj := k.text.value(ptr)
	if obj == nil {
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

func newReflectMapEncoder(base mapEncoderBase) ValEncoder {
	enc := &mapEncoder{mapEncoderBase: base, fresh: exposesAddress(base.typ.Elem())}
	enc.iters.New = func() any {
		return newMapIterator(base.typ)
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
	mapEncoderBase
	fresh bool
	iters sync.Pool
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
		elemPtr := it.elemPtr
		if e.fresh {
			elem := reflect.New(e.typ.Elem())
			elem.Elem().SetIterValue(&it.iter)
			elemPtr = elem.UnsafePointer()
		} else {
			it.elem.SetIterValue(&it.iter)
		}

		e.keys.writeKey(it.keyPtr, w)
		if w.Error != nil {
			break
		}
		e.encoder.Encode(elemPtr, w)
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
