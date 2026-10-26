package ocf_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/require"
)

const benchRecords = 5000

var benchCodecs = []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy, ocf.ZStandard}

func benchRecord() FullRecord {
	unionStr := "union value"
	return FullRecord{
		Strings:  []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:    []int64{1, 2, 3, 4, 5},
		Enum:     "C",
		Map:      map[string]int{"key1": 1, "key2": 2, "key3": 3},
		Nullable: &unionStr,
		Record:   &TestRecord{Long: 1925639126735, String: "I am a test record", Int: 666, Float: 7171.17, Double: 916734926348163.01, Bool: true},
	}
}

func writeBenchFile(b *testing.B, codec ocf.CodecName) []byte {
	b.Helper()

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf, ocf.WithCodec(codec))
	require.NoError(b, err)
	rec := benchRecord()
	for range benchRecords {
		require.NoError(b, enc.Encode(rec))
	}
	require.NoError(b, enc.Close())
	return buf.Bytes()
}

func BenchmarkEncoder(b *testing.B) {
	for _, codec := range benchCodecs {
		b.Run(string(codec), func(b *testing.B) {
			rec := benchRecord()
			buf := &bytes.Buffer{}
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				enc, _ := ocf.NewEncoder(schema, buf, ocf.WithCodec(codec))
				for range benchRecords {
					_ = enc.Encode(rec)
				}
				_ = enc.Close()
			}
		})
	}
}

func BenchmarkDecoder(b *testing.B) {
	for _, codec := range benchCodecs {
		b.Run(string(codec), func(b *testing.B) {
			data := writeBenchFile(b, codec)
			b.ReportAllocs()
			for b.Loop() {
				dec, _ := ocf.NewDecoder(bytes.NewReader(data))
				var rec FullRecord
				for dec.HasNext() {
					_ = dec.Decode(&rec)
				}
				_ = dec.Close()
			}
		})
	}
}
