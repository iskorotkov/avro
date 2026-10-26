package avro

import (
	"errors"
	"math/big"
	"reflect"
	"time"

	"github.com/iskorotkov/avro/v2/internal/unsafex"
)

func genericDecode(typ reflect.Type, box unsafex.Boxer, dec ValDecoder, r *Reader) any {
	ptr := reflect.New(typ).UnsafePointer()
	dec.Decode(ptr, r)
	if r.Error != nil {
		return nil
	}

	obj := box.Box(ptr)
	if unsafex.IsNil(obj) {
		return nil
	}
	return obj
}

func genericReceiver(schema Schema) (reflect.Type, error) {
	if schema.Type() == Ref {
		schema = schema.(*RefSchema).Schema()
	}

	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	schemaName := string(schema.Type())
	if ls != nil {
		schemaName += "." + string(ls.Type())
	}

	switch schema.Type() {
	case Null:
		return reflect.TypeFor[*null](), nil
	case Boolean:
		var v bool
		return reflect.TypeOf(v), nil
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				var v time.Time
				return reflect.TypeOf(v), nil

			case TimeMillis:
				var v time.Duration
				return reflect.TypeOf(v), nil
			}
		}
		var v int
		return reflect.TypeOf(v), nil
	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				var v time.Duration
				return reflect.TypeOf(v), nil
			case TimestampMillis, TimestampMicros, TimestampNanos,
				LocalTimestampMillis, LocalTimestampMicros, LocalTimestampNanos:
				var v time.Time
				return reflect.TypeOf(v), nil
			}
		}
		var v int64
		return reflect.TypeOf(v), nil
	case Float:
		var v float32
		return reflect.TypeOf(v), nil
	case Double:
		var v float64
		return reflect.TypeOf(v), nil
	case String:
		var v string
		return reflect.TypeOf(v), nil
	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			var v *big.Rat
			return reflect.TypeOf(v), nil
		}
		var v []byte
		return reflect.TypeOf(v), nil
	case Record:
		var v map[string]any
		return reflect.TypeOf(v), nil
	case Enum:
		var v string
		return reflect.TypeOf(v), nil
	case Array:
		v := make([]any, 0)
		return reflect.TypeOf(v), nil
	case Map:
		var v map[string]any
		return reflect.TypeOf(v), nil
	case Union:
		var v map[string]any
		return reflect.TypeOf(v), nil
	case Fixed:
		fixed := schema.(*FixedSchema)
		ls := fixed.Logical()
		if ls != nil {
			switch ls.Type() {
			case Duration:
				var v LogicalDuration
				return reflect.TypeOf(v), nil
			case Decimal:
				var v *big.Rat
				return reflect.TypeOf(v), nil
			}
		}
		v := byteSliceToArray(make([]byte, fixed.Size()), fixed.Size())
		return reflect.TypeOf(v), nil
	default:
		// This should not be possible.
		return nil, errors.New("dynamic receiver not found for schema " + schemaName)
	}
}
