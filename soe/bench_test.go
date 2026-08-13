package soe_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/soe"
	"github.com/iskorotkov/avro/v2/soe/internal/testdata"
	"github.com/stretchr/testify/require"
)

func BenchmarkEncodeSmall(b *testing.B) {
	c, err := soe.NewCodec(avro.MustParse(`"int"`))
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Encode(3)
	}
}

func BenchmarkEncodeRecord(b *testing.B) {
	c, err := soe.NewCodec(testdata.StringIntSchema)
	require.NoError(b, err)
	v := testdata.StringInt{StringVal: "abc", IntVal: 123}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Encode(v)
	}
}

func BenchmarkAppendEncodeSmall(b *testing.B) {
	c, err := soe.NewCodec(avro.MustParse(`"int"`))
	require.NoError(b, err)
	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = c.AppendEncode(buf[:0], 3)
	}
}

func BenchmarkAppendEncodeRecord(b *testing.B) {
	c, err := soe.NewCodec(testdata.StringIntSchema)
	require.NoError(b, err)
	v := testdata.StringInt{StringVal: "abc", IntVal: 123}
	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ = c.AppendEncode(buf[:0], v)
	}
}
