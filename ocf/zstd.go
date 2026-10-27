package ocf

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdMinDecoderMemory lets frames use the 8 MiB window RFC 8878 asks decoders to support, because the zstd memory limit also caps the window.
const zstdMinDecoderMemory = 8 << 20

// maxSharedZstdDecoders bounds how many distinct decompressed-size limits get a process-wide shared decoder besides the uncapped one.
const maxSharedZstdDecoders = 8

// Default zstd instances are shared because EncodeAll and DecodeAll are safe for concurrent use.
var (
	defaultZstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		enc, _ := zstd.NewWriter(nil)
		return enc
	})
	defaultZstdDecoder = sync.OnceValue(func() *zstd.Decoder {
		return newSharedZstdDecoder(0)
	})
	defaultZstdDecoders = newZstdDecoderCache(maxSharedZstdDecoders)
)

// sharedZstdDecoder returns the default decoder for maxDecompressed and whether it is shared; an unshared decoder belongs to the caller.
func sharedZstdDecoder(maxDecompressed int) (*zstd.Decoder, bool) {
	if maxDecompressed <= 0 {
		return defaultZstdDecoder(), true
	}
	return defaultZstdDecoders.get(maxDecompressed)
}

// zstdDecoderCache holds at most size shared decoders, keyed by decoder memory limit.
type zstdDecoderCache struct {
	mu       sync.Mutex
	size     int
	decoders map[int]*zstd.Decoder
}

func newZstdDecoderCache(size int) *zstdDecoderCache {
	return &zstdDecoderCache{size: size, decoders: make(map[int]*zstd.Decoder, size)}
}

// get returns the decoder for maxDecompressed and whether it is shared; an unshared decoder belongs to the caller.
func (c *zstdDecoderCache) get(maxDecompressed int) (*zstd.Decoder, bool) {
	limit := zstdDecoderMemory(maxDecompressed)

	c.mu.Lock()
	defer c.mu.Unlock()

	if dec, ok := c.decoders[limit]; ok {
		return dec, true
	}
	dec := newSharedZstdDecoder(maxDecompressed)
	if len(c.decoders) >= c.size {
		return dec, false
	}
	c.decoders[limit] = dec
	return dec, true
}

func (c *zstdDecoderCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.decoders)
}

// zstdDecoderMemory returns the decoder memory limit for maxDecompressed, or 0 for no limit.
func zstdDecoderMemory(maxDecompressed int) int {
	if maxDecompressed <= 0 {
		return 0
	}
	return max(maxDecompressed, zstdMinDecoderMemory)
}

// newSharedZstdDecoder lifts klauspost's default cap of 4 concurrent DecodeAll calls to GOMAXPROCS.
func newSharedZstdDecoder(maxDecompressed int) *zstd.Decoder {
	return newZstdDecoder(maxDecompressed, zstd.WithDecoderConcurrency(0))
}

func newZstdDecoder(maxDecompressed int, opts ...zstd.DOption) *zstd.Decoder {
	if limit := zstdDecoderMemory(maxDecompressed); limit > 0 {
		opts = append([]zstd.DOption{zstd.WithDecoderMaxMemory(uint64(limit))}, opts...)
	}
	dec, _ := zstd.NewReader(nil, opts...)
	return dec
}
