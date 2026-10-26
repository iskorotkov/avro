package avro_test

import (
	"math/big"
	"testing"

	"github.com/iskorotkov/avro/v2"
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

func TestEncoder_SchemasDifferingOnlyInDefaultsDoNotShareEncoders(t *testing.T) {
	type partial struct {
		A int `avro:"a"`
	}
	type wrapper struct {
		Inner partial `avro:"inner"`
	}
	record := func(field string) string {
		return `{"type":"record","name":"D449","fields":[{"name":"a","type":"int"},` + field + `]}`
	}
	nested := func(field string) string {
		return `{"type":"record","name":"W449","fields":[{"name":"inner","type":` + record(field) + `}]}`
	}
	tests := []struct {
		name     string
		schemaA  string
		schemaB  string
		value    any
		wantErrA bool
	}{
		{
			name:     "null default added",
			schemaA:  record(`{"name":"b","type":["null","string"]}`),
			schemaB:  record(`{"name":"b","type":["null","string"],"default":null}`),
			value:    partial{A: 1},
			wantErrA: true,
		},
		{
			name:    "default changed",
			schemaA: record(`{"name":"b","type":"string","default":"x"}`),
			schemaB: record(`{"name":"b","type":"string","default":"y"}`),
			value:   partial{A: 1},
		},
		{
			name:    "nested default changed",
			schemaA: nested(`{"name":"b","type":"string","default":"x"}`),
			schemaB: nested(`{"name":"b","type":"string","default":"y"}`),
			value:   wrapper{Inner: partial{A: 1}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schemaA, err := avro.ParseWithCache(test.schemaA, "", &avro.SchemaCache{})
			require.NoError(t, err)
			schemaB, err := avro.ParseWithCache(test.schemaB, "", &avro.SchemaCache{})
			require.NoError(t, err)
			require.Equal(t, schemaA.Fingerprint(), schemaB.Fingerprint())
			api := avro.Config{}.Freeze()
			uncached := avro.Config{DisableCaching: true}.Freeze()

			gotA, errA := api.Marshal(schemaA, test.value)
			gotB, errB := api.Marshal(schemaB, test.value)

			if test.wantErrA {
				require.Error(t, errA)
			} else {
				require.NoError(t, errA)
				wantA, _ := uncached.Marshal(schemaA, test.value)
				assert.Equal(t, wantA, gotA)
			}
			require.NoError(t, errB)
			wantB, _ := uncached.Marshal(schemaB, test.value)
			assert.Equal(t, wantB, gotB)
			assert.NotEqual(t, gotA, gotB)
		})
	}
}
