package avro_test

import (
	"encoding/hex"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expected values below were produced by Apache Avro Java 1.12.0 for the same schemas.

func TestEmptyUnion_NewUnionSchemaAcceptsNoTypes(t *testing.T) {
	// Java: Schema.createUnion() returns the union [].
	schema, err := avro.NewUnionSchema(nil)

	require.NoError(t, err)
	assert.Empty(t, schema.Types())
	assert.Equal(t, "[]", schema.String())
}

func TestEmptyUnion_FingerprintsMatchJava(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   map[avro.FingerprintType]string
	}{
		{
			name:   "Union",
			schema: `[]`,
			want: map[avro.FingerprintType]string{
				// Java: SchemaNormalization.parsingFingerprint64 and parsingFingerprint("MD5"|"SHA-256").
				avro.CRC64Avro: "eec6e173d23fb9d1",
				avro.MD5:       "d751713988987e9331980363e24189ce",
				avro.SHA256:    "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
			},
		},
		{
			name:   "Record Field",
			schema: `{"type":"record","name":"R","fields":[{"name":"f","type":[]}]}`,
			want: map[avro.FingerprintType]string{
				avro.SHA256: "3094a4c70372e7acff30d89dbd34b458af2d4376873dfadd3642cbe664ef5523",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})
			require.NoError(t, err)

			for typ, want := range test.want {
				got, err := schema.FingerprintUsing(typ)
				require.NoError(t, err)
				assert.Equal(t, want, hex.EncodeToString(got), typ)
			}
		})
	}
}

func TestEmptyUnion_CanonicalFormMatchesJava(t *testing.T) {
	tests := []struct {
		schema string
		want   string
	}{
		{schema: `[]`, want: `[]`},
		{schema: `{"type":"array","items":[]}`, want: `{"type":"array","items":[]}`},
		{
			schema: `{"type":"record","name":"R","fields":[{"name":"f","type":[]}]}`,
			want:   `{"name":"R","type":"record","fields":[{"name":"f","type":[]}]}`,
		},
	}

	for _, test := range tests {
		schema, err := avro.ParseWithCache(test.schema, "", &avro.SchemaCache{})
		require.NoError(t, err)

		assert.Equal(t, test.want, schema.String())
	}
}

func TestEmptyUnion_RejectsEveryDefault(t *testing.T) {
	// Java: AvroTypeException "Invalid default for field f: null not a []".
	_, err := avro.ParseWithCache(`{"type":"record","name":"R","fields":[{"name":"f","type":[],"default":null}]}`, "", &avro.SchemaCache{})

	require.EqualError(t, err, "avro: invalid default for field f. <nil> not a union")
}

func TestEmptyUnion_CompatibilityMatchesJava(t *testing.T) {
	empty := `[]`
	nullInt := `["null","int"]`
	withField := `{"type":"record","name":"R","namespace":"empty.compat","fields":[{"name":"f","type":[]}]}`
	noField := `{"type":"record","name":"R","namespace":"empty.compat","fields":[]}`

	// Java: SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType().
	tests := []struct {
		name    string
		reader  string
		writer  string
		wantErr string
	}{
		{name: "Both Empty", reader: empty, writer: empty},
		{name: "Writer Empty", reader: `"int"`, writer: empty},
		{name: "Writer Empty Reader Union", reader: nullInt, writer: empty},
		{name: "Reader Empty", reader: empty, writer: `"int"`, wantErr: "reader union lacking writer schema int"},
		{name: "Reader Empty Writer Union", reader: empty, writer: nullInt, wantErr: "reader union lacking writer schema null"},
		{name: "Reader Drops Empty Union Field", reader: noField, writer: withField},
		{name: "Reader Adds Empty Union Field", reader: withField, writer: noField, wantErr: "reader field f is missing in writer schema and has no default"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := avro.ParseWithCache(test.reader, "", &avro.SchemaCache{})
			require.NoError(t, err)
			w, err := avro.ParseWithCache(test.writer, "", &avro.SchemaCache{})
			require.NoError(t, err)

			err = avro.NewSchemaCompatibility().Compatible(r, w)

			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.wantErr)
		})
	}
}

func TestEmptyUnion_EncodeFailsForEveryValue(t *testing.T) {
	// Java: GenericDatumWriter throws UnresolvedUnionException "Not in union []".
	schema, err := avro.ParseWithCache(`[]`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	for _, v := range []any{nil, 1, "x"} {
		_, err := avro.Marshal(schema, v)

		assert.ErrorContains(t, err, "avro: unknown union type", v)
	}
}

func TestEmptyUnion_DecodeFailsForEveryIndex(t *testing.T) {
	// Java: GenericDatumReader throws EOFException or ArrayIndexOutOfBoundsException.
	schema, err := avro.ParseWithCache(`[]`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	for _, data := range [][]byte{{}, {0x00}, {0x02}} {
		var got any
		err := avro.Unmarshal(schema, data, &got)

		assert.EqualError(t, err, "avro: decode union type: unknown union type", data)
		assert.Nil(t, got)
	}
}

func TestEmptyUnion_DecodeResolvedWriterFails(t *testing.T) {
	// Java: reading writer [] as reader "int" is compatible but throws ArrayIndexOutOfBoundsException.
	r := avro.MustParse(`"int"`)
	w, err := avro.ParseWithCache(`[]`, "", &avro.SchemaCache{})
	require.NoError(t, err)

	sch, err := avro.NewSchemaCompatibility().Resolve(r, w)
	require.NoError(t, err)

	var got any
	assert.EqualError(t, avro.Unmarshal(sch, []byte{0x00}, &got), "avro: decode union type: unknown union type")
}
