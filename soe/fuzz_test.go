package soe_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
	"github.com/iskorotkov/avro/v2/soe"
	"github.com/iskorotkov/avro/v2/soe/internal/testdata"
)

// FuzzCodecDecode decodes arbitrary single-object payloads and round-trips the valid ones.
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

	decode := func(data []byte) (any, error) {
		var v any
		err := codec.Decode(data, &v)
		return v, err
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		var typed testdata.StringInt
		_ = codec.Decode(data, &typed)
		var generic any
		_ = codec.DecodeUnverified(data, &generic)

		if _, payload, err := soe.ParseHeader(data); err == nil {
			if _, err = fuzzcheck.Decode(avro.DefaultConfig, testdata.StringIntSchema, payload); err == nil {
				fuzzcheck.RoundTrip(t, data, decode, codec.Encode)
			}
		}
	})
}
