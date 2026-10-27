package ocf

import (
	"compress/flate"
	"testing"
)

func BenchmarkCodec(b *testing.B) {
	input := makeTestData(64<<10, func() byte { return 'a' })

	for _, name := range []CodecName{Null, Deflate, Snappy} {
		codec, err := resolveCodec(name, codecOptions{DeflateCompressionLevel: flate.DefaultCompression})
		if err != nil {
			b.Fatal(err)
		}
		encoded := append([]byte(nil), codec.Encode(input)...)

		b.Run(string(name)+"/Encode", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = codec.Encode(input)
			}
		})
		b.Run(string(name)+"/Decode", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := codec.Decode(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
