package resolvers_test

import (
	"context"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/soe"
	"github.com/iskorotkov/avro/v2/soe/resolvers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These names produce fixed schemas with the same CRC-64-AVRO fingerprint.
const (
	collidingNameA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	collidingNameB = "bbababaabbbbaababbbbababbaabaababababbbaabaaaabaababbbababaaaaabbaaaaa"
)

func fixedSchema(t *testing.T, name string) avro.Schema {
	t.Helper()
	schema, err := avro.ParseWithCache(`{"type":"fixed","name":"`+name+`","size":1}`, "", &avro.SchemaCache{})
	require.NoError(t, err)
	return schema
}

func TestMemorySchemaStore_AddSchemaIsIdempotent(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()
	schema := fixedSchema(t, collidingNameA)

	require.NoError(t, store.AddSchema(schema))
	require.NoError(t, store.AddSchema(fixedSchema(t, collidingNameA)))
}

func TestMemorySchemaStore_AddSchemaRejectsCollision(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()
	first := fixedSchema(t, collidingNameA)
	second := fixedSchema(t, collidingNameB)
	fp, err := soe.ComputeFingerprint(first)
	require.NoError(t, err)
	fp2, err := soe.ComputeFingerprint(second)
	require.NoError(t, err)
	require.Equal(t, fp, fp2)

	require.NoError(t, store.AddSchema(first))
	err = store.AddSchema(second)

	require.ErrorIs(t, err, resolvers.ErrFingerprintCollision)
	got, err := store.GetSchema(context.Background(), fp)
	require.NoError(t, err)
	assert.Equal(t, first.String(), got.String())
}

func TestMemorySchemaStore_GetSchemaUnknown(t *testing.T) {
	store := resolvers.NewMemorySchemaStore()

	_, err := store.GetSchema(context.Background(), make([]byte, 8))

	assert.ErrorIs(t, err, soe.ErrUnknownSchema)
}
