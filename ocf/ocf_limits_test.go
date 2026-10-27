package ocf_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
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
