package ocf_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"runtime"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultCap = 64 << 20

// blockClaiming returns an OCF file whose first block header claims size compressed bytes but holds none.
func blockClaiming(t *testing.T, size int64) []byte {
	t.Helper()

	original := buildOCF(t, ocf.Null, []int64{1, 2, 3})
	var hostile bytes.Buffer
	hostile.Write(original[:findFirstBlockHeaderOffset(t, original)])
	writeZigZagLong(&hostile, 1)
	writeZigZagLong(&hostile, size)
	return hostile.Bytes()
}

// snappyClaiming returns an OCF file whose first snappy block claims size decoded bytes.
func snappyClaiming(t *testing.T, size uint64) []byte {
	t.Helper()

	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], size)
	payload := append(hdr[:n:n], 0x00, 0, 0, 0, 0)
	return replaceFirstBlockPayload(t, buildOCF(t, ocf.Snappy, []int64{1, 2, 3}), payload)
}

func decodeError(t *testing.T, data []byte, opts ...ocf.DecoderFunc) error {
	t.Helper()

	dec, err := ocf.NewDecoder(bytes.NewReader(data), opts...)
	require.NoError(t, err)
	defer dec.Close()

	for dec.HasNext() {
		var v int64
		_ = dec.Decode(&v)
	}
	return dec.Error()
}

func TestDecoder_BlockCapsDefaultForZeroOrNegative(t *testing.T) {
	tests := []struct {
		name string
		opts []ocf.DecoderFunc
	}{
		{name: "unset"},
		{name: "zero", opts: []ocf.DecoderFunc{ocf.WithMaxBlockBytes(0), ocf.WithMaxDecompressedBlockBytes(0)}},
		{name: "negative", opts: []ocf.DecoderFunc{ocf.WithMaxBlockBytes(-1), ocf.WithMaxDecompressedBlockBytes(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atLimit := decodeError(t, blockClaiming(t, defaultCap), tt.opts...)
			require.Error(t, atLimit)
			assert.NotContains(t, atLimit.Error(), "WithMaxBlockBytes")

			overLimit := decodeError(t, blockClaiming(t, defaultCap+1), tt.opts...)
			require.ErrorContains(t, overLimit, "WithMaxBlockBytes")

			atDecompressedLimit := decodeError(t, snappyClaiming(t, defaultCap), tt.opts...)
			require.Error(t, atDecompressedLimit)
			assert.NotContains(t, atDecompressedLimit.Error(), "claimed decoded size")

			overDecompressedLimit := decodeError(t, snappyClaiming(t, defaultCap+1), tt.opts...)
			require.ErrorContains(t, overDecompressedLimit, "claimed decoded size")
		})
	}
}

func TestDecoder_ExplicitBlockCapBoundary(t *testing.T) {
	const blockCap = 1000

	atLimit := decodeError(t, blockClaiming(t, blockCap), ocf.WithMaxBlockBytes(blockCap))
	require.Error(t, atLimit)
	assert.NotContains(t, atLimit.Error(), "WithMaxBlockBytes")

	require.ErrorContains(t, decodeError(t, blockClaiming(t, blockCap+1), ocf.WithMaxBlockBytes(blockCap)), "WithMaxBlockBytes")
}

// compressZeros compresses n zero bytes into an OCF block payload for codec.
func compressZeros(t *testing.T, codec ocf.CodecName, n int) []byte {
	t.Helper()

	data := make([]byte, n)
	switch codec {
	case ocf.Deflate:
		return craftDeflate(t, data)
	case ocf.Snappy:
		payload := snappy.Encode(nil, data)
		return binary.BigEndian.AppendUint32(payload, crc32.ChecksumIEEE(data))
	default:
		enc, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		defer enc.Close()
		return enc.EncodeAll(data, nil)
	}
}

func TestDecoder_DefaultDecompressedCapPerCodec(t *testing.T) {
	tests := []struct {
		codec    ocf.CodecName
		isCapErr func(err error) bool
	}{
		{codec: ocf.Deflate, isCapErr: func(err error) bool { return strings.Contains(err.Error(), "decompressed size exceeds") }},
		{codec: ocf.Snappy, isCapErr: func(err error) bool { return strings.Contains(err.Error(), "claimed decoded size") }},
		{codec: ocf.ZStandard, isCapErr: func(err error) bool { return errors.Is(err, zstd.ErrDecoderSizeExceeded) }},
	}
	for _, tt := range tests {
		t.Run(string(tt.codec), func(t *testing.T) {
			original := buildOCF(t, tt.codec, []int64{1, 2, 3})

			// Three zero bytes decode as the three records; the rest of the block is never read.
			require.NoError(t, decodeError(t, replaceFirstBlockPayload(t, original, compressZeros(t, tt.codec, defaultCap))))

			overCap := decodeError(t, replaceFirstBlockPayload(t, original, compressZeros(t, tt.codec, defaultCap+1)))
			require.Error(t, overCap)
			assert.True(t, tt.isCapErr(overCap), "unexpected error: %v", overCap)
		})
	}
}

func TestDecoder_SnappyBombIsRejectedBeforeAllocation(t *testing.T) {
	// Eleven bytes: a varint claiming almost 4 GiB, one literal tag byte, a literal byte and the CRC trailer.
	var bomb []byte
	bomb = binary.AppendUvarint(bomb, 1<<32-1)
	bomb = append(bomb, 0x00, 0x00, 0, 0, 0, 0)
	require.Len(t, bomb, 11)
	data := replaceFirstBlockPayload(t, buildOCF(t, ocf.Snappy, []int64{1, 2, 3}), bomb)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := decodeError(t, data)
	runtime.ReadMemStats(&after)

	// On 32-bit platforms the snappy library rejects the claim itself, before the cap is checked.
	require.ErrorContains(t, err, "snappy:")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}
