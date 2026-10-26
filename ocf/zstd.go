package ocf

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// maxSharedZstdDecoders bounds how many distinct decompressed-size limits get a process-wide shared decoder.
const maxSharedZstdDecoders = 8

// Default zstd instances are shared because EncodeAll and DecodeAll are safe for concurrent use.
var (
	defaultZstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		enc, _ := zstd.NewWriter(nil)
		return enc
	})
	defaultZstdDecoders = newZstdDecoderCache(maxSharedZstdDecoders)
)

// zstdDecoderCache holds at most size shared decoders, keyed by decompressed-size limit.
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if dec, ok := c.decoders[maxDecompressed]; ok {
		return dec, true
	}
	dec := newZstdDecoder(maxDecompressed)
	if len(c.decoders) >= c.size {
		return dec, false
	}
	c.decoders[maxDecompressed] = dec
	return dec, true
}

func (c *zstdDecoderCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.decoders)
}

func newZstdDecoder(maxDecompressed int, opts ...zstd.DOption) *zstd.Decoder {
	if maxDecompressed > 0 {
		opts = append([]zstd.DOption{zstd.WithDecoderMaxMemory(uint64(maxDecompressed))}, opts...)
	}
	dec, _ := zstd.NewReader(nil, opts...)
	return dec
}
