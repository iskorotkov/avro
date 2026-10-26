package avro

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/modern-go/reflect2"
)

type efaceDecoder struct {
	schema Schema
	typ    reflect2.Type
	dec    ValDecoder
}

func newEfaceDecoder(d *decoderContext, schema Schema) *efaceDecoder {
	typ, _ := genericReceiver(schema)
	dec := decoderOfType(d, schema, typ)

	return &efaceDecoder{
		schema: schema,
		typ:    typ,
		dec:    dec,
	}
}

func (d *efaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	pObj := (*any)(ptr)
	d.decode(pObj, r)
	*pObj = r.cfg.typeConverters.decodeConverted(*pObj, d.schema, r)
}

func (d *efaceDecoder) decode(pObj *any, r *Reader) {
	if *pObj == nil {
		*pObj = genericDecode(d.typ, d.dec, r)
		return
	}

	typ := reflect2.TypeOf(*pObj)
	if typ.Kind() != reflect.Pointer {
		*pObj = genericDecode(d.typ, d.dec, r)
		return
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	ptrElemType := ptrType.Elem()
	if reflect2.IsNil(*pObj) {
		obj := ptrElemType.New()
		r.ReadVal(d.schema, obj)
		*pObj = obj
		return
	}
	r.ReadVal(d.schema, *pObj)
}

type interfaceEncoder struct {
	cfg    *frozenConfig
	schema Schema
	typ    reflect2.Type
	cached atomic.Pointer[typedEncoder]
}

// typedEncoder pins the encoder built for the first dynamic type seen by an interfaceEncoder.
type typedEncoder struct {
	rtype   uintptr
	encoder ValEncoder
}

func (e *interfaceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := e.typ.UnsafeIndirect(ptr)

	obj, ok := w.cfg.typeConverters.encodeConverted(obj, e.schema, w)
	if !ok {
		return
	}

	e.encoderOf(obj).Encode(reflect2.PtrOf(obj), w)
}

func (e *interfaceEncoder) encoderOf(obj any) ValEncoder {
	rtype := reflect2.RTypeOf(obj)
	if cached := e.cached.Load(); cached != nil && cached.rtype == rtype {
		return cached.encoder
	}

	enc := e.cfg.EncoderOf(e.schema, reflect2.TypeOf(obj))
	// First type wins so values of mixed types never thrash the slot.
	e.cached.CompareAndSwap(nil, &typedEncoder{rtype: rtype, encoder: enc})
	return enc
}
