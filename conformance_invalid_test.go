package avro_test

import (
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// invalidBinaryVectors returns inputs the spec does not allow; value is the zero value of the typed decode target.
func invalidBinaryVectors() []binaryVector {
	const (
		union = `["null","int"]`
		enum  = `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`
		fixed = `{"type":"fixed","name":"F","size":3}`
	)
	return []binaryVector{
		{"boolean/2", `"boolean"`, false, "02"},
		{"union/index-out-of-range", union, (*int32)(nil), "04"},
		{"union/negative-index", union, (*int32)(nil), "01"},
		{"enum/index-out-of-range", enum, "", "08"},
		{"enum/negative-index", enum, "", "01"},
		{"bytes/negative-length", `"bytes"`, []byte(nil), "01"},
		{"string/negative-length", `"string"`, "", "01"},
		{"int/overlong", `"int"`, int32(0), "808080808001"},
		{"long/overlong", `"long"`, int64(0), "8080808080808080808001"},
		{"time-micros/overflows-duration", `{"type":"long","logicalType":"time-micros"}`, time.Duration(0), "80808080808080808001"},
		{"truncated/empty-int", `"int"`, int32(0), ""},
		{"truncated/int", `"int"`, int32(0), "80"},
		{"truncated/long", `"long"`, int64(0), "ffff"},
		{"truncated/float", `"float"`, float32(0), "0000c0"},
		{"truncated/double", `"double"`, float64(0), "00000000000000"},
		{"truncated/bytes", `"bytes"`, []byte(nil), "0400"},
		{"truncated/string", `"string"`, "", "0661"},
		{"truncated/fixed", fixed, [3]byte{}, "0102"},
		{"truncated/enum", enum, "", ""},
		{"truncated/union-value", union, (*int32)(nil), "02"},
		{"truncated/record", matrixRecordSchema, matrixRecord{}, "36"},
		{"truncated/array", `{"type":"array","items":"long"}`, []int64(nil), "0406"},
		{"truncated/array-end", `{"type":"array","items":"long"}`, []int64(nil), "020636"},
		{"truncated/map-key", `{"type":"map","values":"long"}`, map[string]int64(nil), "0204"},
		{"truncated/map-value", `{"type":"map","values":"long"}`, map[string]int64(nil), "020261"},
	}
}

func TestConformance_InvalidBinary(t *testing.T) {
	xf := loadExpectedFailures(t)
	for _, v := range invalidBinaryVectors() {
		t.Run(v.name, func(t *testing.T) {
			xf.Run(t, "invalid/"+v.name, func(t xfail.T) { assertInvalidBinary(t, v) })
		})
	}
}

func assertInvalidBinary(t xfail.T, v binaryVector) {
	t.Helper()

	schema, err := avro.Parse(v.schema)
	require.NoError(t, err)
	data, err := hex.DecodeString(v.hex)
	require.NoError(t, err)

	out := reflect.New(reflect.TypeOf(v.value))
	assert.Error(t, specAPI.Unmarshal(schema, data, out.Interface()), "typed decode")
	var generic any
	assert.Error(t, specAPI.Unmarshal(schema, data, &generic), "generic decode")
}
