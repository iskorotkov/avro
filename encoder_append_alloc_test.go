//go:build !race

package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendMarshal_ZeroAllocs(t *testing.T) {
	defer ConfigTeardown()

	schema, err := avro.ParseFiles("testdata/superhero.avsc")
	require.NoError(t, err)
	super := newBenchSuperhero()
	buf, err := avro.AppendMarshal(nil, schema, super)
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(100, func() {
		buf, _ = avro.AppendMarshal(buf[:0], schema, super)
	})

	assert.Zero(t, allocs)
}
