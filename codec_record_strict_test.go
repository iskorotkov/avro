package avro_test

import (
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const strictSchema = `{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string","aliases":["c"]}]}`

func strictAPI() avro.API {
	return avro.Config{DisallowUnknownFields: true}.Freeze()
}

func TestDisallowUnknownFields_Matching(t *testing.T) {
	type rec struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
		_ int
		Y int `avro:"-"`
	}
	api := strictAPI()
	schema := avro.MustParse(strictSchema)

	data, err := api.Marshal(schema, rec{A: 1, B: "x"})
	require.NoError(t, err)
	var got rec
	err = api.Unmarshal(schema, data, &got)

	require.NoError(t, err)
	assert.Equal(t, rec{A: 1, B: "x"}, got)
}

func TestDisallowUnknownFields_DecodeAlias(t *testing.T) {
	type rec struct {
		A int64  `avro:"a"`
		C string `avro:"c"`
	}
	schema := avro.MustParse(strictSchema)

	var got rec
	err := strictAPI().Unmarshal(schema, []byte{0x02, 0x02, 'x'}, &got)

	require.NoError(t, err)
	assert.Equal(t, rec{A: 1, C: "x"}, got)
}

func TestDisallowUnknownFields_MissingStructField(t *testing.T) {
	type rec struct {
		A int64 `avro:"a"`
	}
	schema := avro.MustParse(strictSchema)

	_, encErr := strictAPI().Marshal(schema, rec{A: 1})
	decErr := strictAPI().Unmarshal(schema, []byte{0x02, 0x02, 'x'}, &rec{})

	assert.ErrorIs(t, encErr, avro.ErrUnknownField)
	assert.ErrorIs(t, decErr, avro.ErrUnknownField)
}

func TestDisallowUnknownFields_ExtraStructField(t *testing.T) {
	type rec struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
		D bool   `avro:"d"`
	}
	schema := avro.MustParse(strictSchema)

	_, encErr := strictAPI().Marshal(schema, rec{})
	decErr := strictAPI().Unmarshal(schema, []byte{0x02, 0x02, 'x'}, &rec{})

	assert.ErrorIs(t, encErr, avro.ErrUnknownField)
	assert.ErrorIs(t, decErr, avro.ErrUnknownField)
}

func TestDisallowUnknownFields_ResolvedWriterOnlyField(t *testing.T) {
	type rec struct {
		A int64 `avro:"a"`
	}
	writer := avro.MustParse(strictSchema)
	reader := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"}]}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	var got rec
	err = strictAPI().Unmarshal(resolved, []byte{0x02, 0x02, 'x'}, &got)

	require.NoError(t, err)
	assert.Equal(t, rec{A: 1}, got)
}

const strictInnerSchema = `{"type":"record","name":"inner","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}`

type strictInner struct {
	A int64 `avro:"a"`
}

func TestDisallowUnknownFields_NestedWithoutData(t *testing.T) {
	type nested struct {
		R strictInner `avro:"r"`
	}
	type inUnion struct {
		R *strictInner `avro:"r"`
	}
	type inArray struct {
		R []strictInner `avro:"r"`
	}
	type inMap struct {
		R map[string]strictInner `avro:"r"`
	}
	tests := []struct {
		name   string
		field  string
		value  any
		data   []byte
		target func() any
	}{
		{name: "record", field: strictInnerSchema, value: nested{}, data: []byte{0x02, 0x04}, target: func() any { return &nested{} }},
		{name: "union null branch", field: `["null",` + strictInnerSchema + `]`, value: inUnion{}, data: []byte{0x00}, target: func() any { return &inUnion{} }},
		{name: "empty array", field: `{"type":"array","items":` + strictInnerSchema + `}`, value: inArray{}, data: []byte{0x00}, target: func() any { return &inArray{} }},
		{name: "empty map", field: `{"type":"map","values":` + strictInnerSchema + `}`, value: inMap{}, data: []byte{0x00}, target: func() any { return &inMap{} }},
	}
	const wantMsg = `avro: unknown field: record inner field "b" has no matching field in avro_test.strictInner`

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(`{"type":"record","name":"outer","fields":[{"name":"r","type":` + test.field + `}]}`)

			_, encErr := strictAPI().Marshal(schema, test.value)
			decErr := strictAPI().Unmarshal(schema, test.data, test.target())

			require.ErrorIs(t, encErr, avro.ErrUnknownField)
			require.ErrorIs(t, decErr, avro.ErrUnknownField)
			assert.EqualError(t, encErr, wantMsg)
			assert.EqualError(t, decErr, wantMsg)
		})
	}
}

func TestDisallowUnknownFields_LenientNestedWithoutData(t *testing.T) {
	defer ConfigTeardown()

	type inArray struct {
		R []strictInner `avro:"r"`
	}
	schema := avro.MustParse(`{"type":"record","name":"outer","fields":[{"name":"r","type":{"type":"array","items":` + strictInnerSchema + `}}]}`)

	err := avro.Unmarshal(schema, []byte{0x00}, &inArray{})

	assert.NoError(t, err)
}

func TestDisallowUnknownFields_IgnoredWriterFieldIsUnknownToStruct(t *testing.T) {
	type rec struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	writer := avro.MustParse(strictSchema)
	reader := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"}]}`)
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	require.NoError(t, err)

	err = strictAPI().Unmarshal(resolved, []byte{0x02, 0x02, 'x'}, &rec{})

	assert.ErrorIs(t, err, avro.ErrUnknownField)
}

func TestDisallowUnknownFields_NilInNullableUnion(t *testing.T) {
	type rec struct {
		R any `avro:"r"`
	}
	schema := avro.MustParse(`{"type":"record","name":"outer","fields":[{"name":"r","type":["null",` + strictInnerSchema + `]}]}`)

	got, err := strictAPI().Marshal(schema, rec{})
	require.NoError(t, err)
	gotMap, err := strictAPI().Marshal(schema, map[string]any{"r": nil})
	require.NoError(t, err)

	assert.Equal(t, []byte{0x00}, got)
	assert.Equal(t, []byte{0x00}, gotMap)
}

func TestDisallowUnknownFields_DefaultIsLenient(t *testing.T) {
	defer ConfigTeardown()

	type rec struct {
		A int64 `avro:"a"`
		D bool  `avro:"d"`
	}
	schema := avro.MustParse(strictSchema)

	err := avro.Unmarshal(schema, []byte{0x02, 0x02, 'x'}, &rec{})

	assert.NoError(t, err)
}

func TestDisallowUnknownFields_DefaultFieldStillRequired(t *testing.T) {
	type rec struct {
		A int64 `avro:"a"`
	}
	schema := avro.MustParse(`{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string","default":"x"}]}`)

	_, err := strictAPI().Marshal(schema, rec{A: 1})

	assert.ErrorIs(t, err, avro.ErrUnknownField)
}
