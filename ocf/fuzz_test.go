package ocf_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
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
	f.Add([]byte{})
	f.Add([]byte("Obj\x01"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<24 {
			return
		}
		dec, err := ocf.NewDecoder(bytes.NewReader(data), ocf.WithMaxBlockBytes(16<<20))
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
