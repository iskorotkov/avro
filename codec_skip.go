package avro

import (
	"fmt"
	"unsafe"
)

func createSkipDecoder(schema Schema) ValDecoder {
	return createSkipDecoderWithCache(schema, make(map[[32]byte]ValDecoder))
}

func createSkipDecoderWithCache(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	switch schema.Type() {
	case Null:
		return &nullCodec{}

	case Boolean:
		return &boolSkipDecoder{}

	case Int:
		return &intSkipDecoder{}

	case Long:
		return &longSkipDecoder{}

	case Float:
		return &floatSkipDecoder{}

	case Double:
		return &doubleSkipDecoder{}

	case String:
		return &stringSkipDecoder{}

	case Bytes:
		return &bytesSkipDecoder{}

	case Record:
		fingerprint := schema.CacheFingerprint()
		if decoder, ok := cache[fingerprint]; ok {
			return decoder
		}

		deferred := &deferDecoder{}
		cache[fingerprint] = deferred
		deferred.decoder = nestIfRecursive(schema.(*RecordSchema), skipDecoderOfRecord(schema, cache))
		return deferred.decoder

	case Ref:
		ref := schema.(*RefSchema).Schema()
		if decoder, ok := cache[ref.CacheFingerprint()]; ok {
			return decoder
		}
		return createSkipDecoderWithCache(ref, cache)

	case Enum:
		return &enumSkipDecoder{symbols: schema.(*EnumSchema).Symbols()}

	case Array:
		return skipDecoderOfArray(schema, cache)

	case Map:
		return skipDecoderOfMap(schema, cache)

	case Union:
		return skipDecoderOfUnion(schema, cache)

	case Fixed:
		return &fixedSkipDecoder{size: schema.(*FixedSchema).Size()}

	default:
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type boolSkipDecoder struct{}

func (*boolSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBool()
}

type intSkipDecoder struct{}

func (*intSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

type longSkipDecoder struct{}

func (*longSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipLong()
}

type floatSkipDecoder struct{}

func (*floatSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipFloat()
}

type doubleSkipDecoder struct{}

func (*doubleSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipDouble()
}

type stringSkipDecoder struct{}

func (*stringSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipString()
}

type bytesSkipDecoder struct{}

func (c *bytesSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBytes()
}

func skipDecoderOfRecord(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	rec := schema.(*RecordSchema)

	decoders := make([]ValDecoder, len(rec.Fields()))
	for i, field := range rec.Fields() {
		decoders[i] = createSkipDecoderWithCache(field.Type(), cache)
	}

	return &recordSkipDecoder{
		decoders: decoders,
	}
}

type recordSkipDecoder struct {
	decoders []ValDecoder
}

func (d *recordSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for _, decoder := range d.decoders {
		decoder.Decode(nil, r)
		if r.Error != nil {
			return
		}
	}
}

type enumSkipDecoder struct {
	symbols []string
}

func (c *enumSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

func skipDecoderOfArray(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	arr := schema.(*ArraySchema)
	decoder := createSkipDecoderWithCache(arr.Items(), cache)

	return &sliceSkipDecoder{
		decoder:     decoder,
		minItemSize: minWireSize(arr.Items()),
	}
}

type sliceSkipDecoder struct {
	decoder     ValDecoder
	minItemSize int
}

func (d *sliceSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if size > 0 {
			r.SkipNBytes(size)
			continue
		}

		if !r.allowArrayBlock(l, d.minItemSize) {
			return
		}

		for range l {
			d.decoder.Decode(nil, r)
			if r.Error != nil {
				return
			}
		}
	}
}

func skipDecoderOfMap(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	m := schema.(*MapSchema)
	decoder := createSkipDecoderWithCache(m.Values(), cache)

	return &mapSkipDecoder{
		decoder:      decoder,
		minEntrySize: minMapEntrySize(m),
	}
}

type mapSkipDecoder struct {
	decoder      ValDecoder
	minEntrySize int
}

func (d *mapSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if size > 0 {
			r.SkipNBytes(size)
			continue
		}

		if !r.allowMapBlock(l, d.minEntrySize) {
			return
		}

		for range l {
			r.SkipString()
			d.decoder.Decode(nil, r)
			if r.Error != nil {
				return
			}
		}
	}
}

func skipDecoderOfUnion(schema Schema, cache map[[32]byte]ValDecoder) ValDecoder {
	union := schema.(*UnionSchema)

	decoders := make([]ValDecoder, len(union.Types()))
	for i, typ := range union.Types() {
		if typ.Type() == Null {
			continue
		}
		decoders[i] = createSkipDecoderWithCache(typ, cache)
	}

	return &unionSkipDecoder{
		schema:   union,
		decoders: decoders,
	}
}

type unionSkipDecoder struct {
	schema   *UnionSchema
	decoders []ValDecoder
}

func (d *unionSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	idx, resSchema := getUnionSchema(d.schema, r)
	if resSchema == nil {
		return
	}

	// Null branches have no decoder and no payload.
	if d.decoders[idx] == nil {
		return
	}

	d.decoders[idx].Decode(nil, r)
}

type fixedSkipDecoder struct {
	size int
}

func (d *fixedSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipNBytes(d.size)
}
