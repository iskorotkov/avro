package ocf

import (
	"math/rand"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZstdEncodeDecodeLowEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeLowEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

func TestSnappyEncode_ProducesStrictSnappyBlock(t *testing.T) {
	input := makeTestData(8762, func() byte { return byte(rand.Intn(4)) })
	codec, err := resolveCodec(Snappy, codecOptions{})
	require.NoError(t, err)

	compressed := codec.Encode(input)
	got, err := snappy.DecodeStrict(nil, compressed[:len(compressed)-4])

	require.NoError(t, err)
	assert.Equal(t, input, got)
}

/*
benchmark results always creating a new zstd encoder/decoder

goos: linux
goarch: amd64
pkg: github.com/iskorotkov/avro/v2/ocf
cpu: AMD Ryzen 5 3550H with Radeon Vega Mobile Gfx


BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	     289	   3523847 ns/op	10891887 B/op	      40 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	     298	   3390952 ns/op	10894703 B/op	      40 allocs/op


benchmark results reusing an existing zstd encoder/decoder

BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	   55628	     22883 ns/op	   19220 B/op	       2 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	   47652	     25064 ns/op	   31553 B/op	       3 allocs/op
*/

func BenchmarkZstdEncodeDecodeLowEntropyLong(b *testing.B) {
	benchmarkEncodeDecode(b, ZStandard, makeTestData(8762, func() byte { return 'a' }))
}

func BenchmarkZstdEncodeDecodeHighEntropyLong(b *testing.B) {
	benchmarkEncodeDecode(b, ZStandard, makeTestData(8762, func() byte { return byte(rand.Uint32()) }))
}

func BenchmarkSnappyEncodeDecodeLowEntropyLong(b *testing.B) {
	benchmarkEncodeDecode(b, Snappy, makeTestData(8762, func() byte { return 'a' }))
}

func BenchmarkSnappyEncodeDecodeHighEntropyLong(b *testing.B) {
	benchmarkEncodeDecode(b, Snappy, makeTestData(8762, func() byte { return byte(rand.Uint32()) }))
}

func benchmarkEncodeDecode(b *testing.B, name CodecName, input []byte) {
	b.Helper()

	codec, err := resolveCodec(name, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()

	for b.Loop() {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func verifyZstdEncodeDecode(t *testing.T, input []byte) {
	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(t, err)

	compressed := codec.Encode(input)
	actual, decodeErr := codec.Decode(compressed)

	require.NoError(t, decodeErr)
	assert.Equal(t, input, actual)
}

func makeTestData(length int, charMaker func() byte) []byte {
	input := make([]byte, length)
	for i := range length {
		input[i] = charMaker()
	}
	return input
}
