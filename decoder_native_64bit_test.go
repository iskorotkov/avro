//go:build amd64 || arm64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x || wasm

package avro_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 2147483648 (= MaxInt32 + 1) overflows int as an untyped constant on
// 32-bit GOARCH, so this case lives in a 64-bit-only file.
func TestDecoder_Int_Long(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x80, 0x80, 0x80, 0x80, 0x10}
	schema := "long"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, 2147483648, i)
}
