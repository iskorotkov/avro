package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
)

func fuzzTargets() map[string]fuzzTarget {
	return map[string]fuzzTarget{"record": fuzzRecordTarget(), "recursive": fuzzTreeTarget()}
}

func TestDecode_EveryTruncationReturnsError(t *testing.T) {
	defer ConfigTeardown()

	api := avro.Config{}.Freeze()
	for name, target := range fuzzTargets() {
		t.Run(name, func(t *testing.T) {
			for _, seed := range target.seeds {
				data, err := api.Marshal(target.schema, seed)
				assert.NoError(t, err)

				for n := range data {
					for _, o := range decodeAllWays(api, target, data[:n]) {
						assert.Errorf(t, o.err, "prefix of %d/%d bytes decoded without error by %s", n, len(data), o.way)
					}
				}
			}
		})
	}
}

func TestDecode_EveryCorruptedByteAgrees(t *testing.T) {
	defer ConfigTeardown()

	api := avro.Config{}.Freeze()
	for name, target := range fuzzTargets() {
		t.Run(name, func(t *testing.T) {
			for _, seed := range target.seeds {
				data, err := api.Marshal(target.schema, seed)
				assert.NoError(t, err)

				for i, b := range data {
					for _, corrupted := range []byte{0x00, 0x7f, 0x80, 0xff, b ^ 0x01} {
						mutated := append([]byte(nil), data...)
						mutated[i] = corrupted
						checkDecodeAgreement(t, api, target, decodeAllWays(api, target, mutated))
					}
				}
			}
		})
	}
}
