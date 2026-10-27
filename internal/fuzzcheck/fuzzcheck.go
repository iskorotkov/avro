// Package fuzzcheck holds invariants shared by fuzz targets.
package fuzzcheck

import (
	"math"
	"reflect"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

// Decode reads one value of schema through a Reader, so input that ends inside the value is an error.
func Decode(api avro.API, schema avro.Schema, data []byte) (any, error) {
	var v any
	r := avro.NewReader(nil, 0, avro.WithReaderConfig(api)).Reset(data)
	r.ReadVal(schema, &v)
	return v, r.Error
}

// RoundTrip fails t unless a value decoded from data re-encodes and decodes to the same value.
func RoundTrip(t testing.TB, data []byte, decode func([]byte) (any, error), encode func(any) ([]byte, error)) {
	t.Helper()

	first, err := decode(data)
	if err != nil {
		return
	}
	encoded, err := encode(first)
	if err != nil {
		t.Fatalf("re-encode decoded value %#v: %v", first, err)
	}
	second, err := decode(encoded)
	if err != nil {
		t.Fatalf("decode re-encoded %x: %v", encoded, err)
	}
	if !Same(first, second) {
		t.Fatalf("round trip changed the value:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

// Same reports whether a and b are deeply equal, treating NaNs as equal and -0 as distinct from 0.
func Same(a, b any) bool {
	return same(reflect.ValueOf(a), reflect.ValueOf(b))
}

func same(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() {
		return a.IsValid() == b.IsValid()
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		return sameFloat(a.Float(), b.Float())
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return same(a.Elem(), b.Elem())
	case reflect.Slice, reflect.Array:
		return sameElems(a, b)
	case reflect.Map:
		return sameEntries(a, b)
	case reflect.Struct:
		return sameFields(a, b)
	default:
		return a.Equal(b)
	}
}

func sameFloat(x, y float64) bool {
	if math.IsNaN(x) || math.IsNaN(y) {
		return math.IsNaN(x) && math.IsNaN(y)
	}
	return x == y && math.Signbit(x) == math.Signbit(y)
}

func sameElems(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for i := range a.Len() {
		if !same(a.Index(i), b.Index(i)) {
			return false
		}
	}
	return true
}

func sameEntries(a, b reflect.Value) bool {
	if a.Len() != b.Len() {
		return false
	}
	for _, k := range a.MapKeys() {
		if !same(a.MapIndex(k), b.MapIndex(k)) {
			return false
		}
	}
	return true
}

func sameFields(a, b reflect.Value) bool {
	for i := range a.NumField() {
		if !same(a.Field(i), b.Field(i)) {
			return false
		}
	}
	return true
}
