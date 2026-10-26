package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestDecode_EveryTruncationReturnsError(t *testing.T) {
	defer ConfigTeardown()

	api := avro.Config{}.Freeze()
	schema := avro.MustParse(fuzzDecoderSchema)

	for _, seed := range fuzzSeeds() {
		data, err := api.Marshal(schema, seed)
		assert.NoError(t, err)

		for n := range data {
			for i, err := range decodeAllWays(api, schema, data[:n]) {
				assert.Errorf(t, err, "prefix of %d/%d bytes decoded without error by way %d", n, len(data), i)
			}
		}
	}
}

func TestDecode_EveryCorruptedByteTerminates(t *testing.T) {
	defer ConfigTeardown()

	api := avro.Config{}.Freeze()
	schema := avro.MustParse(fuzzDecoderSchema)

	for _, seed := range fuzzSeeds() {
		data, err := api.Marshal(schema, seed)
		assert.NoError(t, err)

		for i, b := range data {
			for _, corrupted := range []byte{0x00, 0x7f, 0x80, 0xff, b ^ 0x01} {
				mutated := append([]byte(nil), data...)
				mutated[i] = corrupted
				decodeAllWays(api, schema, mutated)
			}
		}
	}
}
