package ocf

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"testing"

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
	input := makeTestData(8762, func() byte { return 'a' })

	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()

	for b.Loop() {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func BenchmarkZstdEncodeDecodeHighEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	codec, err := resolveCodec(ZStandard, codecOptions{})
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

func TestCodecs_ReuseBuffersAcrossCalls(t *testing.T) {
	inputs := [][]byte{
		makeTestData(8762, func() byte { return byte(rand.Uint32()) }),
		makeTestData(7, func() byte { return 'a' }),
		makeTestData(100000, func() byte { return 'b' }),
	}

	for _, name := range []CodecName{Null, Deflate, Snappy, ZStandard} {
		t.Run(string(name), func(t *testing.T) {
			codec, err := resolveCodec(name, codecOptions{DeflateCompressionLevel: -1})
			require.NoError(t, err)

			for _, input := range inputs {
				encoded := codec.Encode(input)
				decoded, err := codec.Decode(encoded)

				require.NoError(t, err)
				assert.Equal(t, input, decoded)
			}
		})
	}
}

func TestZStandardCodec_SharesDefaultInstances(t *testing.T) {
	a := newZStandardCodec(zstdOptions{}, 0)
	b := newZStandardCodec(zstdOptions{}, 0)

	assert.Same(t, a.encoder, b.encoder)
	assert.Same(t, a.decoder, b.decoder)
	require.NoError(t, a.Close())

	out, err := b.Decode(b.Encode([]byte("x")))
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), out)
}

func TestZstdDecoderCache_SharesUpToSize(t *testing.T) {
	cache := newZstdDecoderCache(2)

	a, sharedA := cache.get(10)
	b, sharedB := cache.get(20)
	again, sharedAgain := cache.get(10)
	extra, sharedExtra := cache.get(30)
	defer extra.Close()

	assert.True(t, sharedA)
	assert.True(t, sharedB)
	assert.True(t, sharedAgain)
	assert.Same(t, a, again)
	assert.NotSame(t, a, b)
	assert.False(t, sharedExtra)
	assert.Equal(t, 2, cache.len())
}

func TestZStandardCodec_OwnsDecoderPastSharedLimit(t *testing.T) {
	swapDefaultZstdDecoders(t, newZstdDecoderCache(1))
	shared := newZStandardCodec(zstdOptions{}, 16<<20)
	defer shared.Close()

	owned := newZStandardCodec(zstdOptions{}, 32<<20)
	defer owned.Close()
	uncapped := newZStandardCodec(zstdOptions{}, 0)
	defer uncapped.Close()

	assert.True(t, shared.sharedDecoder)
	assert.False(t, owned.sharedDecoder)
	assert.True(t, uncapped.sharedDecoder)
	assert.Same(t, defaultZstdDecoder(), uncapped.decoder)
	assert.Equal(t, 1, defaultZstdDecoders.len())
	out, err := owned.Decode(owned.Encode([]byte("x")))
	require.NoError(t, err)
	assert.Equal(t, []byte("x"), out)
}

// swapDefaultZstdDecoders replaces the process-wide decoder cache for the duration of the test.
func swapDefaultZstdDecoders(t *testing.T, c *zstdDecoderCache) {
	old := defaultZstdDecoders
	defaultZstdDecoders = c
	t.Cleanup(func() { defaultZstdDecoders = old })
}

// Run with -race: codecs built without options share the process-wide default encoder and decoder.
func TestZStandardCodec_DefaultInstancesConcurrent(t *testing.T) {
	const goroutines = 8
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Go(func() {
			codec := newZStandardCodec(zstdOptions{}, 0)
			defer codec.Close()
			for range 50 {
				out, err := codec.Decode(codec.Encode(input))
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(input, out) {
					errs <- errors.New("round trip mismatch")
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
