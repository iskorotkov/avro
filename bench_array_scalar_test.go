package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/iskorotkov/avro/v2"
)

func benchScalarArrayDecode[T any](b *testing.B, schemaStr string, gen func(int) T) {
	b.Helper()

	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			src := make([]T, n)
			for i := range src {
				src[i] = gen(i)
			}

			buf := &bytes.Buffer{}
			enc, err := avro.NewEncoder(schemaStr, buf)
			if err != nil {
				b.Fatal(err)
			}

			if err := enc.Encode(src); err != nil {
				b.Fatal(err)
			}

			payload := buf.Bytes()

			schema, err := avro.Parse(schemaStr)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))

			var got []T
			for b.Loop() {
				got = got[:0]
				dec := avro.NewDecoderForSchema(schema, bytes.NewReader(payload))
				if err := dec.Decode(&got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchScalarArrayEncode[T any](b *testing.B, schemaStr string, gen func(int) T) {
	b.Helper()

	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			src := make([]T, n)
			for i := range src {
				src[i] = gen(i)
			}

			schema, err := avro.Parse(schemaStr)
			if err != nil {
				b.Fatal(err)
			}

			payload, err := avro.Marshal(schema, src)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))

			for b.Loop() {
				if _, err := avro.Marshal(schema, src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Sub-bench naming reflects which varint-width branch in encodeInt the
// generated values exercise after Avro's zigzag transform:
//   1B: zigzag(v) < 0x80                   -> 1-byte fast path
//   2B: zigzag(v) in [0x80, 0x4000)        -> 2-byte fast path
//   Wide: zigzag(v) >= 0x4000              -> binary.AppendUvarint (3-10 bytes)

func BenchmarkArrayEncode_Int32_1B(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(i & 0x3F) })
}

func BenchmarkArrayEncode_Int32_2B(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(0x40 + i&0x1F80) })
}

func BenchmarkArrayEncode_Int32_Wide(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(i*7919 + 1<<24) })
}

func BenchmarkArrayEncode_Int64_1B(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(i & 0x3F) })
}

func BenchmarkArrayEncode_Int64_2B(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(0x40 + i&0x1F80) })
}

func BenchmarkArrayEncode_Int64_Wide(b *testing.B) {
	benchScalarArrayEncode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(i)*7919 + 1<<48 })
}

func BenchmarkArrayDecode_Int32_1B(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(i & 0x3F) })
}

func BenchmarkArrayDecode_Int32_2B(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(0x40 + i&0x1F80) })
}

func BenchmarkArrayDecode_Int32_Wide(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"int"}`, func(i int) int32 { return int32(i*7919 + 1<<24) })
}

func BenchmarkArrayDecode_Int64_1B(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(i & 0x3F) })
}

func BenchmarkArrayDecode_Int64_2B(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(0x40 + i&0x1F80) })
}

func BenchmarkArrayDecode_Int64_Wide(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"long"}`, func(i int) int64 { return int64(i)*7919 + 1<<48 })
}

func BenchmarkArrayDecode_Float32(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"float"}`, func(i int) float32 { return float32(i) + 0.5 })
}

func BenchmarkArrayDecode_Float64(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"double"}`, func(i int) float64 { return float64(i) + 0.5 })
}

func BenchmarkArrayDecode_Bool(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"boolean"}`, func(i int) bool { return i%2 == 0 })
}

func BenchmarkArrayDecode_String(b *testing.B) {
	benchScalarArrayDecode(b, `{"type":"array","items":"string"}`, func(i int) string { return fmt.Sprintf("item-%d", i) })
}
