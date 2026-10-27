package ocf

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var snappyRefNames = []string{"record", "low", "const"}

func readSnappyRef(tb testing.TB, name string) []byte {
	tb.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "snappy-ref", name))
	require.NoError(tb, err)
	return b
}

func withCRC(block, plain []byte) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(block), crc32.ChecksumIEEE(plain))
}

func TestSnappyCodec_DecodesReferenceBlocks(t *testing.T) {
	for _, name := range snappyRefNames {
		t.Run(name, func(t *testing.T) {
			plain := readSnappyRef(t, name+".bin")
			block := readSnappyRef(t, name+".snappy")

			got, err := (&SnappyCodec{}).Decode(withCRC(block, plain))

			require.NoError(t, err)
			assert.Equal(t, plain, got)
		})
	}
}

func TestSnappyCodec_EncodeReadableByStrictDecoder(t *testing.T) {
	for _, name := range snappyRefNames {
		t.Run(name, func(t *testing.T) {
			plain := readSnappyRef(t, name+".bin")

			enc := (&SnappyCodec{}).Encode(plain)
			got, err := snappy.DecodeStrict(nil, enc[:len(enc)-4])

			require.NoError(t, err)
			assert.Equal(t, plain, got)
			assert.Equal(t, crc32.ChecksumIEEE(plain), binary.BigEndian.Uint32(enc[len(enc)-4:]))
		})
	}
}

func TestSnappyCodec_RejectsS2OnlyBlock(t *testing.T) {
	block := readSnappyRef(t, "s2-repeat.s2")
	plain, err := s2.Decode(nil, block)
	require.NoError(t, err)

	_, err = (&SnappyCodec{}).Decode(withCRC(block, plain))

	assert.Error(t, err)
}

func FuzzSnappyCodecDecode(f *testing.F) {
	for _, name := range snappyRefNames {
		f.Add(withCRC(readSnappyRef(f, name+".snappy"), readSnappyRef(f, name+".bin")))
	}
	s2Block := readSnappyRef(f, "s2-repeat.s2")
	s2Plain, err := s2.Decode(nil, s2Block)
	require.NoError(f, err)
	f.Add(withCRC(s2Block, s2Plain))
	f.Add([]byte{0, 0, 0, 0, 0})

	const maxDecoded = 1 << 20
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := (&SnappyCodec{maxDecompressed: maxDecoded}).Decode(data)
		if len(data) < 5 {
			require.Error(t, err)
			return
		}
		block := data[:len(data)-4]
		if n, lenErr := snappy.DecodedLen(block); lenErr == nil && n > maxDecoded {
			require.Error(t, err)
			return
		}

		want, refErr := snappy.DecodeStrict(nil, block)
		refOK := refErr == nil && crc32.ChecksumIEEE(want) == binary.BigEndian.Uint32(data[len(data)-4:])
		require.Equal(t, refOK, err == nil, "strict reference error: %v, codec error: %v", refErr, err)
		if refOK {
			require.Equal(t, want, got)
		}
	})
}
