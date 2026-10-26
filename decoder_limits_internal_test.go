package avro

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_DecodeResetsStaleLimits(t *testing.T) {
	schema := MustParse(`{"type":"array","items":"int"}`)
	dec := Config{MaxSliceAllocSize: 2}.Freeze().NewDecoder(schema, bytes.NewReader([]byte{0x04, 0x02, 0x04, 0x00}))
	// Simulate a decode that panicked mid-value after spending the whole budget.
	dec.r.limits = readerLimits{open: 1, depth: 3, sliceElem: 2}

	var got []int
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, got)
}
