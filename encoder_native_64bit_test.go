//go:build amd64 || arm64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x || wasm

package avro_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 2147483648 as an untyped integer constant overflows int on 32-bit
// GOARCH, so this case lives in a 64-bit-only file.
func TestEncoder_Int64FromInt(t *testing.T) {
	defer ConfigTeardown()

	schema := "long"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(2147483648)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x80, 0x80, 0x80, 0x80, 0x10}, buf.Bytes())
}
