package avro_test

import (
	"bytes"
	"encoding/hex"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// binaryVector pairs a schema and Go value with its expected Avro binary encoding.
type binaryVector struct {
	name   string
	schema string
	value  any
	hex    string
}

type matrixRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type matrixUnionRecord struct {
	U any `avro:"u"`
}

type matrixOuter struct {
	Inner matrixRecord `avro:"inner"`
	Tags  []string     `avro:"tags"`
}

type matrixLinked struct {
	V    int32         `avro:"v"`
	Next *matrixLinked `avro:"next"`
}

// specAPI writes array and map blocks without the optional byte-size header, as in the spec examples.
var specAPI = avro.Config{DisableBlockSizeHeader: true}.Freeze()

const (
	matrixRecordSchema = `{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`
	matrixUUID         = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
)

func primitiveVectors() []binaryVector {
	return []binaryVector{
		{"boolean/false", `"boolean"`, false, "00"},
		{"boolean/true", `"boolean"`, true, "01"},
		{"int/0", `"int"`, int32(0), "00"},
		{"int/-1", `"int"`, int32(-1), "01"},
		{"int/1", `"int"`, int32(1), "02"},
		{"int/-64", `"int"`, int32(-64), "7f"},
		{"int/64", `"int"`, int32(64), "8001"},
		{"int/max", `"int"`, int32(math.MaxInt32), "feffffff0f"},
		{"int/min", `"int"`, int32(math.MinInt32), "ffffffff0f"},
		{"long/0", `"long"`, int64(0), "00"},
		{"long/-2", `"long"`, int64(-2), "03"},
		{"long/max", `"long"`, int64(math.MaxInt64), "feffffffffffffffff01"},
		{"long/min", `"long"`, int64(math.MinInt64), "ffffffffffffffffff01"},
		{"float/1.5", `"float"`, float32(1.5), "0000c03f"},
		{"float/-inf", `"float"`, float32(math.Inf(-1)), "000080ff"},
		{"double/-2", `"double"`, float64(-2), "00000000000000c0"},
		{"double/+inf", `"double"`, math.Inf(1), "000000000000f07f"},
		{"bytes/empty", `"bytes"`, []byte{}, "00"},
		{"bytes/00ff", `"bytes"`, []byte{0x00, 0xff}, "0400ff"},
		{"string/empty", `"string"`, "", "00"},
		{"string/foo", `"string"`, "foo", "06666f6f"},
		{"string/utf8", `"string"`, "é", "04c3a9"},
		{"enum/first", `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`, "A", "00"},
		{"enum/last", `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`, "D", "06"},
		{"fixed/3", `{"type":"fixed","name":"F","size":3}`, [3]byte{1, 2, 3}, "010203"},
	}
}

func logicalVectors() []binaryVector {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 123456789, time.UTC)
	return []binaryVector{
		{"date/2020", `{"type":"int","logicalType":"date"}`, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "ac9d02"},
		{"date/pre-epoch", `{"type":"int","logicalType":"date"}`, time.Date(1870, 1, 1, 0, 0, 0, 0, time.UTC), "d7ba04"},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, time.Hour, "80bab703"},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, 1500 * time.Microsecond, "b817"},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, ts.Truncate(time.Millisecond), "f6a1b7e6eb5b"},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, ts.Truncate(time.Microsecond), "8089e19f98c2cd05"},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, ts, "aab4ae8eb88dcde52b"},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, ts.Truncate(time.Millisecond), "f6a1b7e6eb5b"},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, ts.Truncate(time.Microsecond), "8089e19f98c2cd05"},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, ts, "aab4ae8eb88dcde52b"},
		{"decimal-bytes/positive", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, big.NewRat(1234, 100), "0404d2"},
		{"decimal-bytes/negative", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, big.NewRat(-1, 1), "029c"},
		{"decimal-fixed/negative", `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":9,"scale":2}`, big.NewRat(-1, 1), "ffffff9c"},
		{"uuid", `{"type":"string","logicalType":"uuid"}`, matrixUUID, "48" + hex.EncodeToString([]byte(matrixUUID))},
		{"duration", `{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`, avro.LogicalDuration{Months: 1, Days: 2, Milliseconds: 3}, "010000000200000003000000"},
	}
}

func unionVectors() []binaryVector {
	const multi = `{"type":"record","name":"R","fields":[{"name":"u","type":["null","int","string"]}]}`
	return []binaryVector{
		{"null-first/null", `["null","string"]`, (*string)(nil), "00"},
		{"null-first/value", `["null","string"]`, new("a"), "020261"},
		{"null-last/null", `["string","null"]`, (*string)(nil), "02"},
		{"null-last/value", `["string","null"]`, new("a"), "000261"},
		{"null-record/value", `["null",` + matrixRecordSchema + `]`, &matrixRecord{A: 27, B: "foo"}, "0236" + "06666f6f"},
		{"multi/null", multi, matrixUnionRecord{U: nil}, "00"},
		{"multi/int", multi, matrixUnionRecord{U: 7}, "020e"},
		{"multi/string", multi, matrixUnionRecord{U: "x"}, "040278"},
	}
}

func nestedVectors() []binaryVector {
	return []binaryVector{
		{"record/spec", matrixRecordSchema, matrixRecord{A: 27, B: "foo"}, "36" + "06666f6f"},
		{"array/empty", `{"type":"array","items":"long"}`, []int64{}, "00"},
		{"array/spec", `{"type":"array","items":"long"}`, []int64{3, 27}, "04063600"},
		{"map/single", `{"type":"map","values":"long"}`, map[string]int64{"a": 1}, "0202610200"},
		{"array-of-records", `{"type":"array","items":` + matrixRecordSchema + `}`, []matrixRecord{{A: 1, B: "x"}, {A: 2, B: "y"}}, "04" + "020278" + "040279" + "00"},
		{"map-of-arrays", `{"type":"map","values":{"type":"array","items":"int"}}`, map[string][]int32{"k": {1}}, "02026b" + "020200" + "00"},
		{"array-of-maps", `{"type":"array","items":{"type":"map","values":"string"}}`, []map[string]string{{"k": "v"}}, "02" + "02026b0276" + "00" + "00"},
		{"array-of-unions", `{"type":"array","items":["null","long"]}`, []*int64{nil, new(int64(1))}, "04" + "00" + "0202" + "00"},
		{
			"record-in-record",
			`{"type":"record","name":"Outer","fields":[{"name":"inner","type":` + matrixRecordSchema + `},{"name":"tags","type":{"type":"array","items":"string"}}]}`,
			matrixOuter{Inner: matrixRecord{A: 1, B: ""}, Tags: []string{"t"}},
			"0200" + "02027400",
		},
		{
			"recursive-record",
			`{"type":"record","name":"L","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","L"]}]}`,
			matrixLinked{V: 1, Next: &matrixLinked{V: 2}},
			"02" + "02" + "04" + "00",
		},
	}
}

func TestConformance_BinaryMatrix(t *testing.T) {
	groups := map[string][]binaryVector{
		"primitive": primitiveVectors(),
		"logical":   logicalVectors(),
		"union":     unionVectors(),
		"nested":    nestedVectors(),
	}
	for group, vectors := range groups {
		for _, v := range vectors {
			t.Run(group+"/"+v.name, func(t *testing.T) {
				assertBinaryVector(t, v)
			})
		}
	}
}

func assertBinaryVector(t *testing.T, v binaryVector) {
	t.Helper()

	schema, err := avro.Parse(v.schema)
	require.NoError(t, err)
	want, err := hex.DecodeString(v.hex)
	require.NoError(t, err)

	got, err := specAPI.Marshal(schema, v.value)
	require.NoError(t, err)
	assert.Equal(t, want, got, "encode")

	out := reflect.New(reflect.TypeOf(v.value))
	require.NoError(t, specAPI.Unmarshal(schema, want, out.Interface()))
	assertSameValue(t, v.value, out.Elem().Interface())

	var generic any
	require.NoError(t, specAPI.Unmarshal(schema, want, &generic))
	regot, err := specAPI.Marshal(schema, generic)
	require.NoError(t, err)
	assert.Equal(t, want, regot, "generic re-encode")
}

func assertSameValue(t *testing.T, want, got any) {
	t.Helper()

	switch w := want.(type) {
	case *big.Rat:
		assert.Zero(t, w.Cmp(got.(*big.Rat)), "want %s, got %s", w, got)
	case time.Time:
		assert.True(t, w.Equal(got.(time.Time)), "want %s, got %s", w, got)
	default:
		assert.Equal(t, want, got)
	}
}

func TestConformance_BinaryBlockEncodings(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		hex    string
		want   any
		// isDefault marks the encoding produced by the default config.
		isDefault bool
	}{
		{"array/negative-count-with-size", `{"type":"array","items":"long"}`, "03" + "04" + "0636" + "00", []int64{3, 27}, true},
		{"array/multiple-blocks", `{"type":"array","items":"long"}`, "0206" + "0236" + "00", []int64{3, 27}, false},
		{"map/negative-count-with-size", `{"type":"map","values":"long"}`, "01" + "06" + "026102" + "00", map[string]int64{"a": 1}, true},
		{"map/multiple-blocks", `{"type":"map","values":"long"}`, "02026102" + "02026204" + "00", map[string]int64{"a": 1, "b": 2}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.Parse(test.schema)
			require.NoError(t, err)
			data, err := hex.DecodeString(test.hex)
			require.NoError(t, err)

			out := reflect.New(reflect.TypeOf(test.want))
			dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))
			require.NoError(t, dec.Decode(out.Interface()))
			assert.Equal(t, test.want, out.Elem().Interface())

			if test.isDefault {
				got, err := avro.Marshal(schema, test.want)
				require.NoError(t, err)
				assert.Equal(t, data, got)
			}
		})
	}
}
