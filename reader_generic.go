package avro

import (
	"fmt"
	"reflect"
	"time"
)

// ReadNext reads the next Avro element as a generic interface.
// Input that ends inside the element is reported as an error wrapping io.ErrUnexpectedEOF.
func (r *Reader) ReadNext(schema Schema) any {
	outermost := r.limits.open == 0
	if outermost && r.atCleanEnd(schema) {
		return nil
	}

	v := r.readValue(schema)
	if outermost {
		r.Error = valueError(r.Error)
	}
	return v
}

func (r *Reader) readValue(schema Schema) any {
	r.beginValue()
	defer r.endValue()
	return r.readNext(schema)
}

// atCleanEnd reports whether the input ends before an element that needs at least one byte, leaving io.EOF set.
func (r *Reader) atCleanEnd(schema Schema) bool {
	return r.Error == nil && r.head == r.tail && r.cfg.minWireSizeOf(schema) > 0 && !r.loadMore()
}

func (r *Reader) readNext(schema Schema) any {
	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	switch schema.Type() {
	case Null:
		return nil
	case Boolean:
		return r.ReadBool()
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				i := r.ReadInt()
				sec := int64(i) * int64(24*time.Hour/time.Second)
				return time.Unix(sec, 0).UTC()

			case TimeMillis:
				return time.Duration(r.ReadInt()) * time.Millisecond
			}
		}
		return int(r.ReadInt())
	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				return time.Duration(r.ReadLong()) * time.Microsecond

			case TimestampMillis:
				i := r.ReadLong()
				sec := i / 1e3
				nsec := (i - sec*1e3) * 1e6
				return time.Unix(sec, nsec).UTC()

			case TimestampMicros:
				i := r.ReadLong()
				sec := i / 1e6
				nsec := (i - sec*1e6) * 1e3
				return time.Unix(sec, nsec).UTC()

			case TimestampNanos:
				i := r.ReadLong()
				sec := i / 1e9
				nsec := i - sec*1e9
				return time.Unix(sec, nsec).UTC()

			case LocalTimestampMillis:
				i := r.ReadLong()
				sec := i / 1e3
				nsec := (i - sec*1e3) * 1e6
				return toLocalWall(time.Unix(sec, nsec).UTC())

			case LocalTimestampMicros:
				i := r.ReadLong()
				sec := i / 1e6
				nsec := (i - sec*1e6) * 1e3
				return toLocalWall(time.Unix(sec, nsec).UTC())

			case LocalTimestampNanos:
				i := r.ReadLong()
				sec := i / 1e9
				nsec := i - sec*1e9
				return toLocalWall(time.Unix(sec, nsec).UTC())
			}
		}
		return r.ReadLong()
	case Float:
		return r.ReadFloat()
	case Double:
		return r.ReadDouble()
	case String:
		return r.ReadString()
	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			dec := ls.(*DecimalLogicalSchema)
			return ratFromBytes(r.ReadBytes(), dec.Scale())
		}
		return r.ReadBytes()
	case Record:
		if !r.enterNested() {
			return nil
		}
		fields := schema.(*RecordSchema).Fields()
		obj := make(map[string]any, len(fields))
		for _, field := range fields {
			obj[field.Name()] = r.readNext(field.Type())
		}
		r.leaveNested()
		return obj
	case Ref:
		return r.readNext(schema.(*RefSchema).Schema())
	case Enum:
		symbols := schema.(*EnumSchema).Symbols()
		idx := int(r.ReadInt())
		if idx < 0 || idx >= len(symbols) {
			r.ReportError("Read", "unknown enum symbol")
			return nil
		}
		return symbols[idx]
	case Array:
		items := schema.(*ArraySchema).Items()
		arr := []any{}
		minItemSize := r.cfg.minWireSizeOf(items)
		r.readArrayCB(func(n int) bool { return r.allowArrayBlock(n, minItemSize) }, func(r *Reader) bool {
			elem := r.readNext(items)
			arr = append(arr, elem)
			return true
		})
		return arr
	case Map:
		m := schema.(*MapSchema)
		obj := map[string]any{}
		// Map keys take at least one byte.
		minEntrySize := 1 + r.cfg.minWireSizeOf(m.Values())
		r.readMapCB(func(n int) bool { return r.allowMapBlock(n, minEntrySize) }, func(r *Reader, field string) bool {
			elem := r.readNext(m.Values())
			obj[field] = elem
			return true
		})
		return obj
	case Union:
		types := schema.(*UnionSchema).Types()
		idx64 := r.ReadLong()
		// Compare in int64 to reject values that would truncate on 32-bit
		// (e.g. 1<<32 narrows to 0 and silently selects types[0]).
		if idx64 < 0 || idx64 > int64(len(types)-1) {
			r.ReportError("Read", "unknown union type")
			return nil
		}
		idx := int(idx64)
		schema = types[idx]
		if schema.Type() == Null {
			return nil
		}

		key := schemaTypeName(schema)
		obj := map[string]any{}
		obj[key] = r.readNext(types[idx])
		return obj
	case Fixed:
		size := schema.(*FixedSchema).Size()
		obj := make([]byte, size)
		r.Read(obj)
		if ls != nil && ls.Type() == Decimal {
			dec := ls.(*DecimalLogicalSchema)
			return ratFromBytes(obj, dec.Scale())
		}
		return byteSliceToArray(obj, size)
	default:
		r.ReportError("Read", fmt.Sprintf("unexpected schema type: %v", schema.Type()))
		return nil
	}
}

// ReadArrayCB reads an array with a callback per item.
func (r *Reader) ReadArrayCB(fn func(*Reader) bool) {
	// Callbacks stream items without allocating, so only ReadNext charges the element budget.
	r.readArrayCB(func(int) bool { return true }, fn)
}

func (r *Reader) readArrayCB(allow func(n int) bool, fn func(*Reader) bool) {
	r.beginValue()
	defer r.endValue()
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 || !allow(l) {
			break
		}
		for range l {
			fn(r)
			if r.Error != nil {
				break
			}
		}
	}
}

// ReadMapCB reads a map with a callback per entry.
func (r *Reader) ReadMapCB(fn func(*Reader, string) bool) {
	// Every entry carries a key of at least one byte; the element budget is left to ReadNext.
	r.readMapCB(func(n int) bool { return r.allowBlock("decode map", n, 1) }, fn)
}

func (r *Reader) readMapCB(allow func(n int) bool, fn func(*Reader, string) bool) {
	r.beginValue()
	defer r.endValue()
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 || !allow(l) {
			break
		}
		for range l {
			field := r.ReadString()
			fn(r, field)
			if r.Error != nil {
				break
			}
		}
	}
}

var byteType = reflect.TypeFor[byte]()

func byteSliceToArray(b []byte, size int) any {
	vArr := reflect.New(reflect.ArrayOf(size, byteType)).Elem()
	reflect.Copy(vArr, reflect.ValueOf(b))
	return vArr.Interface()
}
