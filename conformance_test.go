package avro_test

import (
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedFailuresPath lists conformance checks that fail today, each with a reason and a tracking note.
const expectedFailuresPath = "testdata/conformance/expected-failures.json"

func loadExpectedFailures(t *testing.T) xfail.List {
	t.Helper()

	l, err := xfail.Load(expectedFailuresPath)
	require.NoError(t, err)
	return l
}

// conformanceCaseIDs returns the id of every check that consults the expected failures.
func conformanceCaseIDs(t *testing.T) []string {
	t.Helper()

	var ids []string
	for _, v := range binaryVectors() {
		ids = append(ids, "binary/"+v.name)
	}
	for _, v := range invalidBinaryVectors() {
		ids = append(ids, "invalid/"+v.name)
	}
	for _, v := range resolveVectors() {
		ids = append(ids, "resolve/"+v.name, "compat/"+v.name)
	}
	ids = append(ids, resolveCacheID)
	for _, v := range allSchemaVectors(t) {
		ids = append(ids, "schema/"+v.id)
	}
	for _, c := range javaCases(t, "") {
		ids = append(ids, c.ID)
	}
	return ids
}

func TestConformance_ExpectedFailuresAreKnownChecks(t *testing.T) {
	ids := conformanceCaseIDs(t)

	assert.Empty(t, loadExpectedFailures(t).Unknown(ids), "expected failures name checks that do not exist")
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		assert.False(t, seen[id], "duplicate check id %s", id)
		seen[id] = true
	}
}

// assertSameValue compares decoded values, treating floats bit-for-bit so NaN and -0 are checked exactly.
func assertSameValue(t xfail.T, want, got any) {
	t.Helper()

	switch w := want.(type) {
	case *big.Rat:
		assert.Zero(t, w.Cmp(got.(*big.Rat)), "want %s, got %s", w, got)
	case time.Time:
		assert.True(t, w.Equal(got.(time.Time)), "want %s, got %s", w, got)
	case float32:
		assert.Equal(t, math.Float32bits(w), math.Float32bits(got.(float32)), "want %v, got %v", w, got)
	case float64:
		assert.Equal(t, math.Float64bits(w), math.Float64bits(got.(float64)), "want %v, got %v", w, got)
	default:
		assert.Equal(t, want, got)
	}
}
