package ocf_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// craftOCF writes a header for schema followed by one null-codec block with the given count and payload.
func craftOCF(t testing.TB, schema string, count int64, payload []byte) []byte {
	t.Helper()
	sync := [16]byte{1, 2, 3}
	var out bytes.Buffer
	_, err := ocf.NewEncoder(schema, &out, ocf.WithSyncBlock(sync))
	require.NoError(t, err)
	writeZigZagLong(&out, count)
	writeZigZagLong(&out, int64(len(payload)))
	out.Write(payload)
	out.Write(sync[:])
	return out.Bytes()
}

func decodeAllLongs(t testing.TB, data []byte) ([]int64, error) {
	t.Helper()
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	var got []int64
	for dec.HasNext() {
		var v int64
		if err = dec.Decode(&v); err != nil {
			return got, err
		}
		got = append(got, v)
	}
	return got, dec.Error()
}

func TestDecoder_RejectsCountLargerThanBlock(t *testing.T) {
	data := craftOCF(t, secSchema, 1<<62, []byte{2, 4})

	got, err := decodeAllLongs(t, data)

	assert.Empty(t, got)
	assert.ErrorContains(t, err, "does not fit in 2 bytes")
}

func TestDecoder_CapsZeroWidthCount(t *testing.T) {
	data := craftOCF(t, `"null"`, 1<<62, nil)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)

	assert.False(t, dec.HasNext())
	assert.ErrorContains(t, dec.Error(), "zero-width schema")
}

func TestDecoder_AcceptsZeroWidthCountWithinCap(t *testing.T) {
	data := craftOCF(t, `{"type":"record","name":"Empty","fields":[]}`, 3, nil)

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	n := 0
	for dec.HasNext() {
		var v struct{}
		require.NoError(t, dec.Decode(&v))
		n++
	}

	require.NoError(t, dec.Error())
	assert.Equal(t, 3, n)
}

func TestDecoder_RejectsTrailingBlockBytes(t *testing.T) {
	data := craftOCF(t, secSchema, 1, []byte{2, 4})

	got, err := decodeAllLongs(t, data)

	assert.Equal(t, []int64{1}, got)
	assert.ErrorContains(t, err, "bytes left after its last value")
}

func TestDecoder_RejectsBlockEndingMidValue(t *testing.T) {
	data := craftOCF(t, secSchema, 2, []byte{2, 0x80})

	got, err := decodeAllLongs(t, data)

	assert.Equal(t, []int64{1}, got)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestDecoder_RejectsTruncatedSyncMarker(t *testing.T) {
	data := craftOCF(t, secSchema, 1, []byte{2})
	data = data[:len(data)-5]

	got, err := decodeAllLongs(t, data)

	assert.Empty(t, got)
	assert.ErrorContains(t, err, "truncated sync marker")
}

func TestEncoder_FailedEncodeKeepsBlockIntact(t *testing.T) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(secSchema, &buf)
	require.NoError(t, err)

	require.NoError(t, enc.Encode(int64(1)))
	require.Error(t, enc.Encode("not a long"))
	require.NoError(t, enc.Encode(2))
	require.NoError(t, enc.Close())

	got, err := decodeAllLongs(t, buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, got)
}

func TestDecoder_AlternatingTargetTypes(t *testing.T) {
	data := buildOCF(t, ocf.Null, []int64{1, 2, 3, 4})
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)

	var got []int64
	for i := 0; dec.HasNext(); i++ {
		if i%2 == 0 {
			var v int64
			require.NoError(t, dec.Decode(&v))
			got = append(got, v)
			continue
		}
		var v any
		require.NoError(t, dec.Decode(&v))
		got = append(got, v.(int64))
	}

	require.NoError(t, dec.Error())
	assert.Equal(t, []int64{1, 2, 3, 4}, got)
}

func TestDecoder_RejectsNonPointerTarget(t *testing.T) {
	data := buildOCF(t, ocf.Null, []int64{1})
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	require.True(t, dec.HasNext())

	assert.ErrorContains(t, dec.Decode(int64(0)), "can only unmarshal into pointer")
	var v int64
	require.NoError(t, dec.Decode(&v))
	assert.Equal(t, int64(1), v)
}

func TestDecoder_SkipsEmptyBlockMidFile(t *testing.T) {
	data := craftOCF(t, secSchema, 1, []byte{2})
	sync := data[len(data)-16:]
	var buf bytes.Buffer
	buf.Write(data)
	for _, block := range []struct {
		count   int64
		payload []byte
	}{{0, nil}, {0, nil}, {2, []byte{4, 6}}} {
		writeZigZagLong(&buf, block.count)
		writeZigZagLong(&buf, int64(len(block.payload)))
		buf.Write(block.payload)
		buf.Write(sync)
	}

	got, err := decodeAllLongs(t, buf.Bytes())

	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, got)
}
