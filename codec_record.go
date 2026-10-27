package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
)

func createDecoderOfRecord(d *decoderContext, schema Schema, typ reflect.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Struct:
		return decoderOfStruct(d, schema, typ)

	case reflect.Map:
		if typ.Key().Kind() != reflect.String || typ.Elem().Kind() != reflect.Interface {
			break
		}
		return decoderOfRecord(d, schema, typ)

	case reflect.Pointer:
		return decoderOfPtr(d, schema, typ)

	case reflect.Interface:
		if typ.NumMethod() > 0 {
			return &recordIfaceDecoder{schema: schema, box: unsafex.NewBoxer(typ)}
		}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for avro %s", typ.String(), schema.Type())}
}

func createEncoderOfRecord(e *encoderContext, schema *RecordSchema, typ reflect.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Struct:
		return encoderOfStruct(e, schema, typ)

	case reflect.Map:
		if typ.Key().Kind() != reflect.String || typ.Elem().Kind() != reflect.Interface {
			break
		}
		return encoderOfRecord(e, schema, typ)

	case reflect.Pointer:
		return encoderOfPtr(e, schema, typ)
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for avro %s", typ.String(), schema.Type())}
}

func decoderOfStruct(d *decoderContext, schema Schema, typ reflect.Type) ValDecoder {
	rec := schema.(*RecordSchema)
	structDesc := describeStruct(d.cfg.getTagKey(), typ)

	fields := make([]structFieldDecoder, 0, len(rec.Fields()))

	for _, field := range rec.Fields() {
		if field.action == FieldIgnore {
			fields = append(fields, structFieldDecoder{
				decoder: createSkipDecoder(field.Type()),
			})
			continue
		}

		sf := structDesc.Fields.Get(field.Name())
		if sf == nil {
			for _, alias := range field.Aliases() {
				sf = structDesc.Fields.Get(alias)
				if sf != nil {
					break
				}
			}
		}
		// Skip field if it doesn't exist
		if sf == nil {
			// If the field value doesn't exist in the binary, ignore it instead of
			// appending a 'SkipDecoder'.
			//
			// Note: 'SkipDecoder' performs a read and moves the cursor, which,
			// in this case, will lead to a dirty read.
			if field.action == FieldSetDefault {
				continue
			}

			fields = append(fields, structFieldDecoder{
				decoder: createSkipDecoder(field.Type()),
			})
			continue
		}

		if field.action == FieldSetDefault {
			if field.hasDef {
				fields = append(fields, newStructFieldDecoder(sf.Field, createDefaultDecoder(d, field, sf.Field[len(sf.Field)-1].typ)))

				continue
			}
		}

		dec := decoderOfType(d, field.Type(), sf.Field[len(sf.Field)-1].typ)
		fields = append(fields, newStructFieldDecoder(sf.Field, dec))
	}

	return &structDecoder{typ: typ, fields: fields}
}

// structFieldStep locates a struct field relative to its parent struct.
type structFieldStep struct {
	name      string
	offset    uintptr
	typ       reflect.Type
	isPointer bool
}

// structFieldPath locates a field inside a struct, possibly through embedded structs.
type structFieldPath []structFieldStep

// direct reports whether the field is reached by a single offset from the struct.
func (p structFieldPath) direct() bool {
	return len(p) == 1
}

// decodePtr returns a pointer to the field, allocating nil embedded pointers on the way.
func (p structFieldPath) decodePtr(ptr unsafe.Pointer) unsafe.Pointer {
	for i, step := range p {
		ptr = unsafe.Add(ptr, step.offset)
		if i == len(p)-1 || !step.isPointer {
			continue
		}

		if *(*unsafe.Pointer)(ptr) == nil {
			*(*unsafe.Pointer)(ptr) = reflect.New(step.typ.Elem()).UnsafePointer()
		}
		ptr = *(*unsafe.Pointer)(ptr)
	}

	return ptr
}

// encodePtr returns a pointer to the field, or the name of the nil embedded pointer on the way.
func (p structFieldPath) encodePtr(ptr unsafe.Pointer) (unsafe.Pointer, string) {
	for i, step := range p {
		ptr = unsafe.Add(ptr, step.offset)
		if i == len(p)-1 || !step.isPointer {
			continue
		}

		if *(*unsafe.Pointer)(ptr) == nil {
			return nil, step.name
		}
		ptr = *(*unsafe.Pointer)(ptr)
	}

	return ptr, ""
}

type structFieldDecoder struct {
	field   structFieldPath
	offset  uintptr
	decoder ValDecoder
}

func newStructFieldDecoder(field structFieldPath, decoder ValDecoder) structFieldDecoder {
	return structFieldDecoder{field: field, offset: field[0].offset, decoder: decoder}
}

type structDecoder struct {
	typ    reflect.Type
	fields []structFieldDecoder
}

func (d *structDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	for i := range d.fields {
		field := &d.fields[i]

		var fieldPtr unsafe.Pointer
		switch {
		case field.field == nil:
			field.decoder.Decode(nil, r)
			continue
		case field.field.direct():
			fieldPtr = unsafe.Add(ptr, field.offset)
		default:
			fieldPtr = field.field.decodePtr(ptr)
		}
		field.decoder.Decode(fieldPtr, r)

		if r.Error != nil && !errors.Is(r.Error, io.EOF) {
			r.Error = fmt.Errorf("%s: %w", field.field[0].name, r.Error)
			return
		}
	}
}

func encoderOfStruct(e *encoderContext, rec *RecordSchema, typ reflect.Type) ValEncoder {
	structDesc := describeStruct(e.cfg.getTagKey(), typ)

	fields := make([]structFieldEncoder, 0, len(rec.Fields()))
	for _, field := range rec.Fields() {
		sf := structDesc.Fields.Get(field.Name())
		if sf != nil {
			fields = append(fields, structFieldEncoder{
				field:   sf.Field,
				offset:  sf.Field[0].offset,
				encoder: encoderOfType(e, field.Type(), sf.Field[len(sf.Field)-1].typ),
			})
			continue
		}

		if !field.HasDefault() {
			// In all other cases, this is a required field
			err := fmt.Errorf("avro: record %s is missing required field %q", rec.FullName(), field.Name())
			return &errorEncoder{err: err}
		}

		def := field.Default()
		if field.Default() == nil {
			if field.Type().Type() == Null {
				// We write nothing in a Null case, just skip it
				continue
			}

			if field.Type().Type() == Union && field.Type().(*UnionSchema).Nullable() {
				defaultType := reflect.TypeOf(&def)
				fields = append(fields, structFieldEncoder{
					defaultPtr: unsafe.Pointer(&def),
					encoder:    encoderOfNullableUnion(e, field.Type(), defaultType),
				})
				continue
			}
		}

		defaultType := reflect.TypeOf(def)
		defaultEncoder := encoderOfType(e, field.Type(), defaultType)
		defaultEncoder = directEncoder(defaultType, defaultEncoder)
		fields = append(fields, structFieldEncoder{
			defaultPtr: unsafex.DataOf(def),
			encoder:    defaultEncoder,
		})
	}
	return &structEncoder{typ: typ, fields: fields}
}

type structFieldEncoder struct {
	field      structFieldPath
	offset     uintptr
	defaultPtr unsafe.Pointer
	encoder    ValEncoder
}

type structEncoder struct {
	typ    reflect.Type
	fields []structFieldEncoder
}

func (e *structEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	for i := range e.fields {
		field := &e.fields[i]

		var fieldPtr unsafe.Pointer
		switch {
		case field.field == nil:
			field.encoder.Encode(field.defaultPtr, w)
			continue
		case field.field.direct():
			fieldPtr = unsafe.Add(ptr, field.offset)
		default:
			var nilName string
			if fieldPtr, nilName = field.field.encodePtr(ptr); fieldPtr == nil {
				w.Error = fmt.Errorf("embedded field %q is nil", nilName)
				return
			}
		}
		field.encoder.Encode(fieldPtr, w)

		if w.Error != nil && !errors.Is(w.Error, io.EOF) {
			w.Error = fmt.Errorf("%s: %w", field.field[0].name, w.Error)
			return
		}
	}
}

func decoderOfRecord(d *decoderContext, schema Schema, typ reflect.Type) ValDecoder {
	rec := schema.(*RecordSchema)

	fields := make([]recordMapDecoderField, len(rec.Fields()))
	for i, field := range rec.Fields() {
		switch field.action {
		case FieldIgnore:
			fields[i] = recordMapDecoderField{
				name:    field.Name(),
				decoder: createSkipDecoder(field.Type()),
				skip:    true,
			}
			continue
		case FieldSetDefault:
			if field.hasDef {
				fields[i] = recordMapDecoderField{
					name:    field.Name(),
					decoder: createDefaultDecoder(d, field, typ.Elem()),
				}
				continue
			}
		}

		fields[i] = recordMapDecoderField{
			name:    field.Name(),
			decoder: decoderOfType(d, field.Type(), typ.Elem()),
		}
	}

	return &recordMapDecoder{
		typ:    typ,
		fields: fields,
	}
}

type recordMapDecoderField struct {
	name    string
	decoder ValDecoder
	skip    bool
}

type recordMapDecoder struct {
	typ    reflect.Type
	fields []recordMapDecoderField
}

func (d *recordMapDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	makeMapIfNil(d.typ, ptr, len(d.fields))

	for _, field := range d.fields {
		if field.skip {
			field.decoder.Decode(nil, r)
			continue
		}

		decodeMapEntry(d.typ, ptr, field.name, field.decoder, r)
	}

	wrapMapDecodeError(r, d.typ)
}

func encoderOfRecord(e *encoderContext, rec *RecordSchema, typ reflect.Type) ValEncoder {
	fields := make([]mapEncoderField, len(rec.Fields()))
	for i, field := range rec.Fields() {
		fields[i] = mapEncoderField{
			name:    field.Name(),
			key:     reflect.ValueOf(field.Name()).Convert(typ.Key()),
			hasDef:  field.HasDefault(),
			def:     field.Default(),
			encoder: encoderOfType(e, field.Type(), typ.Elem()),
		}

		if field.HasDefault() {
			switch {
			case field.Type().Type() == Union:
				union := field.Type().(*UnionSchema)
				fields[i].def = map[string]any{
					string(union.Types()[0].Type()): field.Default(),
				}
			case field.Default() == nil:
				continue
			}

			defaultType := reflect.TypeOf(fields[i].def)
			fields[i].defEncoder = encoderOfType(e, field.Type(), defaultType)
			fields[i].defEncoder = directEncoder(defaultType, fields[i].defEncoder)
		}
	}

	return &recordMapEncoder{
		typ:    typ,
		fields: fields,
		native: typ.Key().Kind() == reflect.String && typ.Elem().Kind() == reflect.Interface && typ.Elem().NumMethod() == 0,
	}
}

type mapEncoderField struct {
	name       string
	key        reflect.Value
	hasDef     bool
	def        any
	defEncoder ValEncoder
	encoder    ValEncoder
}

type recordMapEncoder struct {
	typ    reflect.Type
	fields []mapEncoderField
	// native is set when the map shares the layout and string hashing of map[string]any.
	native bool
}

func (e *recordMapEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	native := e.native
	var m, elem reflect.Value
	if !native {
		m = unsafex.MapOf(e.typ, ptr)
		elem = reflect.New(e.typ.Elem())
	}

	for i := range e.fields {
		field := &e.fields[i]

		var val any
		var valPtr unsafe.Pointer
		if native {
			var ok bool
			if val, ok = (*(*map[string]any)(ptr))[field.name]; ok {
				valPtr = noescape(unsafe.Pointer(&val))
			}
		} else if v := m.MapIndex(field.key); v.IsValid() {
			elem.Elem().Set(v)
			valPtr = elem.UnsafePointer()
		}

		if valPtr == nil {
			// Missing required field
			if !field.hasDef {
				w.Error = fmt.Errorf("avro: missing required field %s", field.name)
				return
			}

			// Null default
			if field.def == nil {
				continue
			}

			field.defEncoder.Encode(unsafex.DataOf(field.def), w)
			continue
		}

		field.encoder.Encode(valPtr, w)

		if w.Error != nil && !errors.Is(w.Error, io.EOF) {
			w.Error = fmt.Errorf("%s: %w", field.name, w.Error)
			return
		}
	}
}

type recordIfaceDecoder struct {
	schema Schema
	box    unsafex.Boxer
}

func (d *recordIfaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := d.box.Box(ptr)
	if unsafex.IsNil(obj) {
		r.ReportError("decode non empty interface", "can not unmarshal into nil")
		return
	}

	r.ReadVal(d.schema, obj)
}

type structDescriptor struct {
	Type   reflect.Type
	Fields structFields
}

type structFields []*structField

func (sf structFields) Get(name string) *structField {
	for _, f := range sf {
		if f.Name == name {
			return f
		}
	}

	return nil
}

type structField struct {
	Name  string
	Field structFieldPath

	anon reflect.Type
}

func describeStruct(tagKey string, typ reflect.Type) *structDescriptor {
	fields := structFields{}

	var curr []structField
	next := []structField{{anon: typ}}

	visited := map[reflect.Type]bool{}

	for len(next) > 0 {
		curr, next = next, curr[:0]

		for _, f := range curr {
			if visited[f.anon] {
				continue
			}
			visited[f.anon] = true

			for field := range f.anon.Fields() {
				isUnexported := field.PkgPath != ""

				chain := make([]structFieldStep, len(f.Field)+1)
				copy(chain, f.Field)
				chain[len(f.Field)] = structFieldStep{
					name:      field.Name,
					offset:    field.Offset,
					typ:       field.Type,
					isPointer: field.Type.Kind() == reflect.Pointer,
				}

				if field.Anonymous {
					t := field.Type
					if t.Kind() == reflect.Pointer {
						t = t.Elem()
					}
					if t.Kind() != reflect.Struct {
						continue
					}

					next = append(next, structField{Field: chain, anon: t})
					continue
				}

				// Ignore unexported fields.
				if isUnexported {
					continue
				}

				fieldName := field.Name
				if tag, ok := field.Tag.Lookup(tagKey); ok {
					fieldName, _, _ = strings.Cut(tag, ",")
				}

				fields = append(fields, &structField{
					Name:  fieldName,
					Field: chain,
				})
			}
		}
	}

	return &structDescriptor{
		Type:   typ,
		Fields: fields,
	}
}
