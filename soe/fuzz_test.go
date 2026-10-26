package soe_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2/soe"
	"github.com/iskorotkov/avro/v2/soe/internal/testdata"
)

// FuzzCodecDecode decodes arbitrary single-object-encoded payloads with and without fingerprint checks.
func FuzzCodecDecode(f *testing.F) {
	codec, err := soe.NewCodec(testdata.StringIntSchema)
	if err != nil {
		f.Fatalf("new codec: %v", err)
	}
	seed, err := codec.Encode(testdata.StringInt{StringVal: "abc", IntVal: 123})
	if err != nil {
		f.Fatalf("encode seed: %v", err)
	}
	f.Add(seed)
	f.Add(seed[:10])
	f.Add([]byte{0xc3, 0x01})
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		var typed testdata.StringInt
		_ = codec.Decode(data, &typed)
		var generic any
		_ = codec.DecodeUnverified(data, &generic)
	})
}
