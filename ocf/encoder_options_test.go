package ocf_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeLongs encodes n records with a fixed sync marker and returns the data blocks and their number.
func encodeLongs(t *testing.T, n int, opts ...ocf.EncoderFunc) ([]byte, int) {
	t.Helper()

	sync := [16]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(secSchema, &buf, append([]ocf.EncoderFunc{ocf.WithSyncBlock(sync)}, opts...)...)
	require.NoError(t, err)
	for i := range n {
		require.NoError(t, enc.Encode(int64(i)))
	}
	require.NoError(t, enc.Close())
	// Header metadata order varies between runs, so only the data blocks, each ending with a sync marker, are compared.
	blocks := buf.Bytes()[findFirstBlockHeaderOffset(t, buf.Bytes()):]
	return blocks, bytes.Count(blocks, sync[:])
}

func TestEncoder_BlockOptionsDefaultForZeroOrNegative(t *testing.T) {
	want, blocks := encodeLongs(t, 250)
	require.Equal(t, 3, blocks)

	tests := []struct {
		name string
		opts []ocf.EncoderFunc
	}{
		{name: "explicit default", opts: []ocf.EncoderFunc{ocf.WithBlockLength(100)}},
		{name: "zero length", opts: []ocf.EncoderFunc{ocf.WithBlockLength(0)}},
		{name: "negative length", opts: []ocf.EncoderFunc{ocf.WithBlockLength(-1)}},
		{name: "zero size", opts: []ocf.EncoderFunc{ocf.WithBlockSize(0)}},
		{name: "negative size", opts: []ocf.EncoderFunc{ocf.WithBlockSize(-1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := encodeLongs(t, 250, tt.opts...)

			assert.Equal(t, want, got)
		})
	}
}
