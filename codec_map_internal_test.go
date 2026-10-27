package avro

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type (
	nativeMapKey   string
	nativeMapInt16 int16
)

func TestEncoderOfMap_SelectsNativeEncoder(t *testing.T) {
	tests := []struct {
		typ    reflect.Type
		values string
		want   string
	}{
		{typ: reflect.TypeFor[map[string]string](), values: `"string"`, want: "[string,string]"},
		{typ: reflect.TypeFor[map[nativeMapKey]nativeMapInt16](), values: `"int"`, want: "[string,uint16]"},
		{typ: reflect.TypeFor[map[string]bool](), values: `"boolean"`, want: "[string,uint8]"},
		{typ: reflect.TypeFor[map[string]float64](), values: `"double"`, want: "[string,uint64]"},
		{typ: reflect.TypeFor[map[string][]int](), values: `{"type":"array","items":"int"}`, want: "[string,[]uint8]"},
		{typ: reflect.TypeFor[map[string]*int64](), values: `["null","long"]`, want: "[string,unsafe.Pointer]"},
		{typ: reflect.TypeFor[map[string]map[string]int64](), values: `{"type":"map","values":"long"}`, want: "[string,unsafe.Pointer]"},
		{typ: reflect.TypeFor[map[string]any](), values: `"long"`, want: "[string,interface {}]"},
		{typ: reflect.TypeFor[map[string]time.Time](), values: `{"type":"long","logicalType":"timestamp-micros"}`, want: "[string,time.Time]"},
		{typ: reflect.TypeFor[map[*textPtrInt]int64](), values: `"long"`, want: "[unsafe.Pointer,uint64]"},
		{typ: reflect.TypeFor[map[time.Time]int64](), values: `"long"`, want: "[time.Time,uint64]"},
	}

	for _, test := range tests {
		t.Run(test.typ.String(), func(t *testing.T) {
			enc := testMapEncoder(t, test.typ, test.values)

			assert.Equal(t, "*avro.nativeMapEncoder"+test.want, fmt.Sprintf("%T", enc))
		})
	}
}

func TestEncoderOfMap_FallsBackToReflection(t *testing.T) {
	tests := []struct {
		typ    reflect.Type
		values string
	}{
		{typ: reflect.TypeFor[map[string]struct{ A, B int64 }](), values: `{"type":"record","name":"r","fields":[{"name":"A","type":"long"},{"name":"B","type":"long"}]}`},
		{typ: reflect.TypeFor[map[string]textPtrInt](), values: `"string"`},
		{typ: reflect.TypeFor[map[string]fmt.Stringer](), values: `"string"`},
	}

	for _, test := range tests {
		t.Run(test.typ.String(), func(t *testing.T) {
			enc := testMapEncoder(t, test.typ, test.values)

			assert.IsType(t, &mapEncoder{}, enc)
		})
	}
}

func TestSameLayout_IgnoresPointerPositions(t *testing.T) {
	type ptrFirst struct {
		P *int
		X uintptr
	}
	type ptrLast struct {
		X uintptr
		P *int
	}

	assert.True(t, sameLayout(reflect.TypeFor[ptrFirst](), reflect.TypeFor[ptrLast]()))
	assert.True(t, sameLayout(reflect.TypeFor[ptrFirst](), reflect.TypeFor[[2]unsafe.Pointer]()))
}

func testMapEncoder(t *testing.T, typ reflect.Type, values string) ValEncoder {
	t.Helper()
	schema := MustParse(`{"type":"map","values":` + values + `}`)

	return encoderOfType(newEncoderContext(Config{}.Freeze().(*frozenConfig)), schema, typ)
}

func TestNativeMapEncoderOf_RejectsUnsupportedLayouts(t *testing.T) {
	arrays := mapEncoderBase{typ: reflect.TypeFor[map[string][2]int64]()}
	ints := mapEncoderBase{typ: reflect.TypeFor[map[string]uint32]()}

	assert.Nil(t, nativeMapEncoderOf[string](arrays))
	assert.Nil(t, newNativeMapEncoder[string, uint64](ints))
}
