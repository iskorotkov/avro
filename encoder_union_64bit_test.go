//go:build amd64 || arm64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x || wasm

package avro_test

import (
	"bytes"
	"testing"

	"github.com/iskorotkov/avro/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// int(2147483648) overflows int as a constant on 32-bit, so this case
// from TestEncoder_UnionResolver lives in a 64-bit-only file.
func TestEncoder_UnionResolver_GoIntAsAvroLong(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null","long"]`
	want := []byte{0x2, 0x80, 0x80, 0x80, 0x80, 0x10}

	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int(2147483648))

	require.NoError(t, err)
	assert.Equal(t, want, buf.Bytes())
}
