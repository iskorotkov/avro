package ocf_test

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"io"
	"math"
	"runtime"
	"sync"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const secSchema = `"long"`

// buildOCF writes a single-block OCF file for tests that then mutate it into hostile shapes (truncated blocks, bad sync, negative count).
func buildOCF(t testing.TB, codec ocf.CodecName, records []int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(secSchema, &buf, ocf.WithCodec(codec), ocf.WithBlockLength(len(records)+1))
	require.NoError(t, err)
	for _, v := range records {
		require.NoError(t, enc.Encode(v))
	}
	require.NoError(t, enc.Close())
	return buf.Bytes()
}

func TestDecoder_RejectsDeflateZipBomb(t *testing.T) {
	// Replace the first block payload of a legitimate deflate OCF with a deflate bomb of 1 MiB of zeros.
	original := buildOCF(t, ocf.Deflate, []int64{1, 2, 3})

	bomb := craftDeflate(t, bytes.Repeat([]byte{0}, 1<<20))
	mutated := replaceFirstBlockPayload(t, original, bomb)

	dec, err := ocf.NewDecoder(bytes.NewReader(mutated), ocf.WithMaxDecompressedBlockBytes(64<<10))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.Error(t, dec.Error())
	assert.Contains(t, dec.Error().Error(), "deflate: decompressed size exceeds")
}

func TestDecoder_RejectsSnappyInflatedHeader(t *testing.T) {
	original := buildOCF(t, ocf.Snappy, []int64{1, 2, 3})

	// Forge a snappy block whose varint claims 1 GiB.
	const claimed = 1 << 30
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(claimed))
	bomb := append([]byte{}, hdr[:n]...)
	bomb = append(bomb, 0x00)
	bomb = append(bomb, 0, 0, 0, 0) // CRC trailer

	mutated := replaceFirstBlockPayload(t, original, bomb)

	dec, err := ocf.NewDecoder(bytes.NewReader(mutated), ocf.WithMaxDecompressedBlockBytes(1<<20))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.Error(t, dec.Error())
	assert.Contains(t, dec.Error().Error(), "snappy: claimed decoded size")
}

func TestDecoder_RejectsZstdBomb(t *testing.T) {
	original := buildOCF(t, ocf.ZStandard, []int64{1, 2, 3})

	// 4 MiB of zeros compresses to a handful of bytes with zstd.
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	bomb := enc.EncodeAll(bytes.Repeat([]byte{0}, 4<<20), nil)
	require.NoError(t, enc.Close())

	mutated := replaceFirstBlockPayload(t, original, bomb)

	dec, err := ocf.NewDecoder(bytes.NewReader(mutated), ocf.WithMaxDecompressedBlockBytes(1<<20))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.ErrorIs(t, dec.Error(), zstd.ErrDecoderSizeExceeded)
}

func TestDecoder_RejectsZstdBombViaSharedDecoder(t *testing.T) {
	original := buildOCF(t, ocf.ZStandard, []int64{1, 2, 3})

	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	bomb := enc.EncodeAll(bytes.Repeat([]byte{0}, 2<<20), nil)
	require.NoError(t, enc.Close())
	mutated := replaceFirstBlockPayload(t, original, bomb)

	shared, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer shared.Close()

	dec, err := ocf.NewDecoder(
		bytes.NewReader(mutated),
		ocf.WithZStandardDecoder(shared),
		ocf.WithMaxDecompressedBlockBytes(1<<20),
	)
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.Error(t, dec.Error())
	assert.Contains(t, dec.Error().Error(), "zstd: decompressed size")
}

func TestDecoder_TruncatedBlockDoesNotPreAllocate(t *testing.T) {
	// Splice a block header claiming 1 GiB of payload but with only 8 real bytes: the decoder must surface an unexpected-EOF error without allocating the claimed gigabyte.
	original := buildOCF(t, ocf.Null, []int64{1, 2, 3})
	headerEnd := findFirstBlockHeaderOffset(t, original)

	var hostile bytes.Buffer
	hostile.Write(original[:headerEnd])
	// count = 1 (zigzag 0x02), size = 1 GiB (zigzag long)
	writeZigZagLong(&hostile, 1)
	writeZigZagLong(&hostile, 1<<30)
	hostile.Write(bytes.Repeat([]byte{0xAA}, 8)) // tiny truncated body

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	dec, err := ocf.NewDecoder(bytes.NewReader(hostile.Bytes()), ocf.WithMaxBlockBytes(math.MaxInt))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.Error(t, dec.Error())

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	allocated := int64(after.TotalAlloc - before.TotalAlloc)
	// Soft assertion: a 100 MiB ceiling catches a pre-allocate regression while leaving headroom for normal test noise.
	assert.Less(t, allocated, int64(100<<20), "decoder pre-allocated for a truncated block")
}

func TestDecoder_BadSyncRejectedBeforeDecompress(t *testing.T) {
	// Forge a block that fails both the decompression cap and the sync check: sync is validated first, so "invalid block" must win over "decompressed size exceeds".
	original := buildOCF(t, ocf.Deflate, []int64{1, 2, 3})

	// Body: 4 MiB of zeros deflate-compressed, well past the 1 KiB decompressed cap.
	body := craftDeflate(t, bytes.Repeat([]byte{0}, 4<<20))
	mutated := replaceFirstBlockPayload(t, original, body)
	// Now corrupt the sync that replaceFirstBlockPayload wrote.
	mutated[len(mutated)-1] ^= 0xFF

	dec, err := ocf.NewDecoder(
		bytes.NewReader(mutated),
		ocf.WithMaxDecompressedBlockBytes(1<<10),
	)
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	err = dec.Error()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid block",
		"sync must be checked before decompression — got a non-sync error, "+
			"likely meaning the decompression cap fired first")
	assert.NotContains(t, err.Error(), "decompressed size exceeds",
		"decompression ran before sync validation — ordering regression")
}

func TestDecoder_RejectsNegativeCount(t *testing.T) {
	original := buildOCF(t, ocf.Null, []int64{1, 2, 3})
	headerEnd := findFirstBlockHeaderOffset(t, original)

	mutated := append([]byte{}, original[:headerEnd]...)
	writeZigZagLong((*bufferAt)(&mutated), -1)     // negative count
	writeZigZagLong((*bufferAt)(&mutated), 0)      // zero-sized payload
	mutated = append(mutated, make([]byte, 16)...) // sync placeholder

	dec, err := ocf.NewDecoder(bytes.NewReader(mutated))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.Error(t, dec.Error())
	assert.Contains(t, dec.Error().Error(), "negative record count")
}

// Run with -race: pins that a *zstd.Decoder shared via WithZStandardDecoder is safe under concurrent OCF decoders.
// Regression guard for the old `defer decoder.Reset(nil)`, whose concurrent Reset(nil) calls raced on the shared decoder's state.
func TestSharedZstdDecoder_Concurrent(t *testing.T) {
	const goroutines = 8
	const decodesPerGoroutine = 25

	schema := `{"type": "string"}`
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf, ocf.WithCodec(ocf.ZStandard))
	require.NoError(t, err)
	for range 50 {
		require.NoError(t, enc.Encode("payload for concurrent decode"))
	}
	require.NoError(t, enc.Close())
	encoded := buf.Bytes()

	sharedDecoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer sharedDecoder.Close()

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*decodesPerGoroutine)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range decodesPerGoroutine {
				dec, err := ocf.NewDecoder(
					bytes.NewReader(encoded),
					ocf.WithZStandardDecoder(sharedDecoder),
				)
				if err != nil {
					errs <- err
					return
				}
				for dec.HasNext() {
					var s string
					if err := dec.Decode(&s); err != nil {
						errs <- err
						_ = dec.Close()
						return
					}
				}
				if err := dec.Error(); err != nil {
					errs <- err
				}
				_ = dec.Close()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent decode failed: %v", err)
	}
}

// Encoder-side counterpart: regression guard for the old `defer encoder.Reset(nil)`, which raced with concurrent EncodeAll/Reset on a shared encoder.
func TestSharedZstdEncoder_Concurrent(t *testing.T) {
	const goroutines = 8
	const encodesPerGoroutine = 25

	schema := `{"type": "string"}`
	sharedEncoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer sharedEncoder.Close()

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*encodesPerGoroutine)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range encodesPerGoroutine {
				var buf bytes.Buffer
				enc, err := ocf.NewEncoder(
					schema, &buf,
					ocf.WithCodec(ocf.ZStandard),
					ocf.WithZStandardEncoder(sharedEncoder),
				)
				if err != nil {
					errs <- err
					return
				}
				if err := enc.Encode("payload for concurrent encode"); err != nil {
					errs <- err
					_ = enc.Close()
					return
				}
				if err := enc.Close(); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent encode failed: %v", err)
	}
}

func TestDecoder_HappyPathWithBothCaps(t *testing.T) {
	for _, codec := range []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy, ocf.ZStandard} {
		t.Run(string(codec), func(t *testing.T) {
			original := buildOCF(t, codec, []int64{42, 43, 44, 45, 46})

			dec, err := ocf.NewDecoder(
				bytes.NewReader(original),
				ocf.WithMaxBlockBytes(1<<20),
				ocf.WithMaxDecompressedBlockBytes(1<<20),
			)
			require.NoError(t, err)
			defer dec.Close()

			var got []int64
			for dec.HasNext() {
				var v int64
				require.NoError(t, dec.Decode(&v))
				got = append(got, v)
			}
			require.NoError(t, dec.Error())
			assert.Equal(t, []int64{42, 43, 44, 45, 46}, got)
		})
	}
}

func TestDecoder_DeflateCapAtMaxIntDoesNotOverflow(t *testing.T) {
	original := buildOCF(t, ocf.Deflate, []int64{1, 2, 3})

	dec, err := ocf.NewDecoder(bytes.NewReader(original), ocf.WithMaxDecompressedBlockBytes(math.MaxInt))
	require.NoError(t, err)
	defer dec.Close()

	var got []int64
	for dec.HasNext() {
		var v int64
		require.NoError(t, dec.Decode(&v))
		got = append(got, v)
	}
	require.NoError(t, dec.Error())
	assert.Equal(t, []int64{1, 2, 3}, got)
}

func TestDecoder_DeflateDecompressedSizeBoundary(t *testing.T) {
	original := buildOCF(t, ocf.Deflate, []int64{1, 2, 3})
	body := craftDeflate(t, bytes.Repeat([]byte{0}, 1024))
	mutated := replaceFirstBlockPayload(t, original, body)

	t.Run("exactly at cap decodes", func(t *testing.T) {
		dec, err := ocf.NewDecoder(bytes.NewReader(mutated), ocf.WithMaxDecompressedBlockBytes(1024))
		require.NoError(t, err)
		defer dec.Close()

		n := 0
		for dec.HasNext() {
			var v int64
			require.NoError(t, dec.Decode(&v))
			n++
		}
		require.NoError(t, dec.Error())
		assert.Equal(t, 3, n)
	})

	t.Run("one byte over cap rejected", func(t *testing.T) {
		dec, err := ocf.NewDecoder(bytes.NewReader(mutated), ocf.WithMaxDecompressedBlockBytes(1023))
		require.NoError(t, err)
		defer dec.Close()

		assert.False(t, dec.HasNext())
		require.Error(t, dec.Error())
		assert.Contains(t, dec.Error().Error(), "deflate: decompressed size exceeds")
	})
}

func TestDecoder_ZeroCountBlockSkippedWithoutDecompress(t *testing.T) {
	original := buildOCF(t, ocf.Deflate, []int64{1, 2, 3})
	headerEnd := findFirstBlockHeaderOffset(t, original)
	sync := original[headerEnd-16 : headerEnd]

	bomb := craftDeflate(t, bytes.Repeat([]byte{0}, 1<<20))

	var hostile bytes.Buffer
	hostile.Write(original[:headerEnd])
	writeZigZagLong(&hostile, 0)
	writeZigZagLong(&hostile, int64(len(bomb)))
	hostile.Write(bomb)
	hostile.Write(sync)

	dec, err := ocf.NewDecoder(bytes.NewReader(hostile.Bytes()), ocf.WithMaxDecompressedBlockBytes(1<<10))
	require.NoError(t, err)
	defer dec.Close()

	assert.False(t, dec.HasNext())
	require.NoError(t, dec.Error())
}

// findFirstBlockHeaderOffset returns the offset of the first block-count varint, just past the header sync marker.
func findFirstBlockHeaderOffset(t testing.TB, data []byte) int {
	t.Helper()
	r := avro.NewReader(bytes.NewReader(data), 0)
	var h ocf.Header
	r.ReadVal(ocf.HeaderSchema, &h)
	require.NoError(t, r.Error)
	// The avro reader buffers ahead, so locate the header sync in the raw bytes instead of trusting the reader position.
	idx := bytes.Index(data[4:], h.Sync[:])
	require.GreaterOrEqual(t, idx, 0, "could not locate header sync in OCF bytes")
	return 4 + idx + 16
}

// replaceFirstBlockPayload re-emits an OCF stream with the first block's payload swapped for newPayload, preserving the original count and sync.
func replaceFirstBlockPayload(t testing.TB, original, newPayload []byte) []byte {
	t.Helper()
	r := avro.NewReader(bytes.NewReader(original), 0)
	var h ocf.Header
	r.ReadVal(ocf.HeaderSchema, &h)
	require.NoError(t, r.Error)

	headerEnd := findFirstBlockHeaderOffset(t, original)
	br := avro.NewReader(bytes.NewReader(original[headerEnd:]), 0)
	count := br.ReadLong()
	_ = br.ReadLong() // original size, discarded
	require.NoError(t, br.Error)

	var out bytes.Buffer
	out.Write(original[:headerEnd])
	writeZigZagLong(&out, count)
	writeZigZagLong(&out, int64(len(newPayload)))
	out.Write(newPayload)
	out.Write(h.Sync[:])
	return out.Bytes()
}

// craftDeflate returns the deflate-compressed form of payload for use as an OCF block body.
func craftDeflate(t testing.TB, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.BestCompression)
	require.NoError(t, err)
	_, err = fw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, fw.Close())
	return buf.Bytes()
}

func writeZigZagLong(w io.Writer, v int64) {
	u := uint64(v<<1) ^ uint64(v>>63)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], u)
	_, _ = w.Write(tmp[:n])
}

// bufferAt adapts a []byte to io.Writer for append-style writes.
type bufferAt []byte

func (b *bufferAt) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return len(p), nil
}
