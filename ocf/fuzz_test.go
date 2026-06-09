package ocf_test

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/klauspost/compress/zstd"
)

const fuzzOCFSchema = `{
	"type": "record",
	"name": "FuzzOCFRecord",
	"namespace": "fuzz",
	"fields": [
		{"name": "a", "type": "long"},
		{"name": "b", "type": "string"},
		{"name": "c", "type": ["null", "long"], "default": null}
	]
}`

type fuzzOCFRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
	C *int64 `avro:"c"`
}

// fuzzMaxRecords bounds the per-input decode loop so fuzz frame counts can't translate into long runs without real decoder work.
const fuzzMaxRecords = 1024

func FuzzOCFReader(f *testing.F) {
	avro.DefaultConfig = avro.Config{
		MaxByteSliceSize:  1 << 20,
		MaxSliceAllocSize: 1 << 22,
		MaxMapAllocSize:   1 << 20,
	}.Freeze()
	defer func() { avro.DefaultConfig = avro.Config{}.Freeze() }()

	for _, codec := range []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy} {
		seed, err := buildOCFSeed(codec)
		if err != nil {
			f.Fatalf("build seed (%s): %v", codec, err)
		}
		f.Add(seed)
	}
	for _, seed := range buildBombSeeds() {
		f.Add(seed)
	}
	f.Add([]byte{})
	f.Add([]byte("Obj\x01"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<24 {
			return
		}
		dec, err := ocf.NewDecoder(
			bytes.NewReader(data),
			ocf.WithMaxBlockBytes(16<<20),
			ocf.WithMaxDecompressedBlockBytes(8<<20),
		)
		if err != nil {
			return
		}
		defer dec.Close()

		for n := 0; n < fuzzMaxRecords && dec.HasNext(); n++ {
			var rec fuzzOCFRecord
			if err := dec.Decode(&rec); err != nil {
				return
			}
		}
		_ = dec.Error()
	})
}

// buildBombSeeds returns OCF byte streams whose compressed blocks are
// designed to amplify well past the 8 MiB decompressed cap used in the fuzz
// target. Feeding these to the fuzzer pins the rejection paths as
// interesting input and surfaces regressions where the cap stops firing.
func buildBombSeeds() [][]byte {
	var seeds [][]byte

	// Deflate bomb: ~16 KiB of compressed → 16 MiB of zeros.
	if seed, ok := bombSeedDeflate(); ok {
		seeds = append(seeds, seed)
	}
	// Snappy bomb: inflated varint header claims 1 GiB.
	if seed, ok := bombSeedSnappy(); ok {
		seeds = append(seeds, seed)
	}
	// Zstd bomb: ~150 B compressed → 64 MiB of zeros.
	if seed, ok := bombSeedZstd(); ok {
		seeds = append(seeds, seed)
	}
	return seeds
}

func bombSeedDeflate() ([]byte, bool) {
	original, err := buildOCFSeed(ocf.Deflate)
	if err != nil {
		return nil, false
	}
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return nil, false
	}
	if _, err = fw.Write(bytes.Repeat([]byte{0}, 1<<24)); err != nil {
		return nil, false
	}
	if err = fw.Close(); err != nil {
		return nil, false
	}
	return spliceBlockPayload(original, buf.Bytes()), true
}

func bombSeedSnappy() ([]byte, bool) {
	original, err := buildOCFSeed(ocf.Snappy)
	if err != nil {
		return nil, false
	}
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], 1<<30)
	bomb := append([]byte{}, hdr[:n]...)
	bomb = append(bomb, 0x00)
	bomb = append(bomb, 0, 0, 0, 0)
	return spliceBlockPayload(original, bomb), true
}

func bombSeedZstd() ([]byte, bool) {
	original, err := buildOCFSeed(ocf.ZStandard)
	if err != nil {
		return nil, false
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, false
	}
	bomb := enc.EncodeAll(bytes.Repeat([]byte{0}, 1<<26), nil)
	_ = enc.Close()
	return spliceBlockPayload(original, bomb), true
}

// spliceBlockPayload locates the first data block in an OCF byte stream and
// swaps its payload for newPayload, preserving the existing count and sync.
// Returns the original bytes unchanged if anything looks malformed — the
// fuzzer can tolerate identical seeds.
func spliceBlockPayload(original, newPayload []byte) []byte {
	if len(original) < 4+16 {
		return original
	}
	headerSchema := ocf.HeaderSchema
	r := avro.NewReader(bytes.NewReader(original), 0)
	var h ocf.Header
	r.ReadVal(headerSchema, &h)
	if r.Error != nil {
		return original
	}
	idx := bytes.Index(original[4:], h.Sync[:])
	if idx < 0 {
		return original
	}
	headerEnd := 4 + idx + 16

	br := avro.NewReader(bytes.NewReader(original[headerEnd:]), 0)
	count := br.ReadLong()
	_ = br.ReadLong()
	if br.Error != nil {
		return original
	}

	var out bytes.Buffer
	out.Write(original[:headerEnd])
	writeFuzzZigZag(&out, count)
	writeFuzzZigZag(&out, int64(len(newPayload)))
	out.Write(newPayload)
	out.Write(h.Sync[:])
	return out.Bytes()
}

func writeFuzzZigZag(buf *bytes.Buffer, v int64) {
	u := uint64(v<<1) ^ uint64(v>>63)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], u)
	buf.Write(tmp[:n])
}

func buildOCFSeed(codec ocf.CodecName) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(fuzzOCFSchema, &buf, ocf.WithCodec(codec))
	if err != nil {
		return nil, err
	}

	v := int64(42)
	records := []fuzzOCFRecord{
		{A: 1, B: "alpha", C: nil},
		{A: -1, B: "", C: &v},
		{A: 1 << 30, B: "longer string with spaces", C: nil},
	}
	for _, r := range records {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}

	if err := enc.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
