package avro

import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
)

type efaceDecoder struct {
	schema Schema
	alloc  func() unsafe.Pointer
	box    unsafex.Boxer
	dec    ValDecoder
}

func newEfaceDecoder(d *decoderContext, schema Schema) *efaceDecoder {
	typ, _ := genericReceiver(schema)
	dec := decoderOfType(d, schema, typ)

	return &efaceDecoder{
		schema: schema,
		alloc:  genericAllocator(typ),
		box:    unsafex.NewBoxer(typ),
		dec:    dec,
	}
}

func (d *efaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	pObj := (*any)(ptr)

	defer func() {
		obj, err := r.cfg.typeConverters.DecodeTypeConvert(*pObj, d.schema)
		if err != nil && !errors.Is(err, errNoTypeConverter) {
			r.Error = err
		}
		*pObj = obj
	}()

	if *pObj == nil {
		*pObj = genericDecode(d.alloc, d.box, d.dec, r)
		return
	}

	typ := reflect.TypeOf(*pObj)
	if typ.Kind() != reflect.Pointer {
		*pObj = genericDecode(d.alloc, d.box, d.dec, r)
		return
	}

	if unsafex.IsNil(*pObj) {
		obj := reflect.New(typ.Elem()).Interface()
		r.ReadVal(d.schema, obj)
		*pObj = obj
		return
	}
	r.ReadVal(d.schema, *pObj)
}

type interfaceEncoder struct {
	schema Schema
	box    unsafex.Boxer
}

func (e *interfaceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := e.box.Box(ptr)

	obj, err := w.cfg.typeConverters.EncodeTypeConvert(obj, e.schema)
	if err != nil && !errors.Is(err, errNoTypeConverter) {
		w.Error = err
		return
	}

	w.WriteVal(e.schema, obj)
}
