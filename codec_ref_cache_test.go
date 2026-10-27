package avro_test

import (
	"math/big"
	"testing"

	"github.com/iskorotkov/avro/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var refCacheCases = []struct {
	name       string
	defA, defB string
	value      any
}{
	{
		name:  "decimal scale",
		defA:  `{"type":"fixed","name":"T","size":8,"logicalType":"decimal","precision":10,"scale":6}`,
		defB:  `{"type":"fixed","name":"T","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
		value: big.NewRat(3, 2),
	},
	{
		name:  "enum symbols",
		defA:  `{"type":"enum","name":"T","symbols":["X","Y"]}`,
		defB:  `{"type":"enum","name":"T","symbols":["Y","X"]}`,
		value: "X",
	},
	{
		name:  "record fields",
		defA:  `{"type":"record","name":"T","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
		defB:  `{"type":"record","name":"T","fields":[{"name":"b","type":"string"},{"name":"a","type":"int"}]}`,
		value: map[string]any{"a": 1, "b": "s"},
	},
}

var refCacheContainers = []struct {
	name   string
	schema string
	wrap   func(any) any
}{
	{name: "array", schema: `{"type":"array","items":"T"}`, wrap: func(v any) any { return []any{v} }},
	{name: "map", schema: `{"type":"map","values":"T"}`, wrap: func(v any) any { return map[string]any{"k": v} }},
	{name: "union", schema: `["null","T"]`, wrap: func(v any) any { return map[string]any{"T": v} }},
}

func parseRefCacheSchema(t *testing.T, def, container string) avro.Schema {
	t.Helper()

	schema, err := avro.ParseWithCache(`{"type":"record","name":"top","fields":[
		{"name":"def","type":["null",`+def+`]},
		{"name":"use","type":`+container+`}
	]}`, "", &avro.SchemaCache{})
	require.NoError(t, err)
	return schema
}

func TestCodec_SameNamedTypeDifferentDefinitions(t *testing.T) {
	for _, tc := range refCacheCases {
		for _, c := range refCacheContainers {
			t.Run(tc.name+"/"+c.name, func(t *testing.T) {
				defer ConfigTeardown()

				a := parseRefCacheSchema(t, tc.defA, c.schema)
				b := parseRefCacheSchema(t, tc.defB, c.schema)
				rec := map[string]any{"def": nil, "use": c.wrap(tc.value)}
				isolated := avro.Config{}.Freeze()
				wantData, err := isolated.Marshal(b, rec)
				require.NoError(t, err)
				var want map[string]any
				require.NoError(t, isolated.Unmarshal(b, wantData, &want))

				dataA, err := avro.Marshal(a, rec)
				require.NoError(t, err)
				var outA map[string]any
				require.NoError(t, avro.Unmarshal(a, dataA, &outA))
				gotData, err := avro.Marshal(b, rec)
				require.NoError(t, err)
				var got map[string]any
				err = avro.Unmarshal(b, wantData, &got)

				require.NoError(t, err)
				assert.Equal(t, wantData, gotData)
				assert.Equal(t, want, got)
			})
		}
	}
}

func TestCacheFingerprint_ExpandsExternalReferences(t *testing.T) {
	a := parseRefCacheSchema(t, refCacheCases[0].defA, `{"type":"array","items":"T"}`)
	b := parseRefCacheSchema(t, refCacheCases[0].defB, `{"type":"array","items":"T"}`)
	useA := a.(*avro.RecordSchema).Fields()[1].Type()
	useB := b.(*avro.RecordSchema).Fields()[1].Type()

	assert.Equal(t, useA.Fingerprint(), useB.Fingerprint())
	assert.NotEqual(t, useA.CacheFingerprint(), useB.CacheFingerprint())
	assert.Equal(t, a.Fingerprint(), a.CacheFingerprint())
}
