package avro

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExternalRefsIdentity(t *testing.T) {
	enum, err := NewEnumSchema("E", "", []string{"A"})
	require.NoError(t, err)
	other, err := NewEnumSchema("O", "", []string{"B"})
	require.NoError(t, err)
	inner, err := NewRecordSchema("I", "", []*Field{mustNewField(t, "o", NewRefSchema(other))})
	require.NoError(t, err)
	enumFP, otherFP, innerFP := enum.Fingerprint(), other.Fingerprint(), inner.Fingerprint()

	tests := []struct {
		name   string
		schema Schema
		want   []byte
	}{
		{name: "primitive", schema: NewPrimitiveSchema(Int, nil), want: nil},
		{name: "definition", schema: enum, want: nil},
		{name: "external ref", schema: NewArraySchema(NewRefSchema(enum)), want: enumFP[:]},
		{
			name:   "ref after definition",
			schema: mustNewUnion(t, NewArraySchema(enum), NewMapSchema(NewRefSchema(enum))),
			want:   nil,
		},
		{
			name:   "duplicate external ref",
			schema: mustNewUnion(t, NewArraySchema(NewRefSchema(enum)), NewMapSchema(NewRefSchema(enum))),
			want:   enumFP[:],
		},
		{
			name:   "transitive external ref",
			schema: NewArraySchema(NewRefSchema(inner)),
			want:   append(append([]byte{}, innerFP[:]...), otherFP[:]...),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, externalRefsIdentity(tt.schema))
		})
	}
}

func TestExternalRefsIdentity_RecursiveRecord(t *testing.T) {
	schema, err := Parse(`{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]}]}`)
	require.NoError(t, err)

	assert.Empty(t, externalRefsIdentity(schema))
	assert.Equal(t, schema.Fingerprint(), schema.CacheFingerprint())
}

func mustNewField(t *testing.T, name string, typ Schema) *Field {
	t.Helper()

	f, err := NewField(name, typ)
	require.NoError(t, err)
	return f
}

func mustNewUnion(t *testing.T, types ...Schema) *UnionSchema {
	t.Helper()

	u, err := NewUnionSchema(types)
	require.NoError(t, err)
	return u
}

func TestDefaultsIdentity_NonJSONDefaults(t *testing.T) {
	record := func(def float64) Schema {
		t.Helper()
		f, err := NewField("d", NewPrimitiveSchema(Double, nil), WithDefault(def))
		require.NoError(t, err)
		rec, err := NewRecordSchema("R", "identity.nan", []*Field{f})
		require.NoError(t, err)
		return rec
	}

	nan, inf := defaultsIdentity(record(math.NaN())), defaultsIdentity(record(math.Inf(1)))

	assert.Equal(t, `"identity.nan.R"."d"=3:NaN`, string(nan))
	assert.Equal(t, `"identity.nan.R"."d"=4:+Inf`, string(inf))
	assert.NotEqual(t, record(math.NaN()).CacheFingerprint(), record(math.Inf(1)).CacheFingerprint())
}
