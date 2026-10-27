package avro_test

import (
	"encoding/hex"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/fuzzcheck"
	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resolveVector reads writer-encoded data with a reader schema; want is the reader encoding, empty when reading fails.
type resolveVector struct {
	name       string
	writer     string
	reader     string
	hex        string
	want       string
	compatible bool
}

const (
	resolveWriterRecord = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	resolveEnumABC      = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
)

// readerWithField returns a reader record that adds field x of the given type and default to resolveWriterRecord.
func readerWithField(typ, def string) string {
	return `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"x","type":` + typ + `,"default":` + def + `}]}`
}

// writerWithField returns a writer record with an extra field x before a, which a reader of resolveWriterRecord skips.
func writerWithField(typ string) string {
	return `{"type":"record","name":"R","fields":[{"name":"x","type":` + typ + `},{"name":"a","type":"int"}]}`
}

// wantHex returns the expected reader encoding, or nil when reading fails.
func (v resolveVector) wantHex() *string {
	if v.want == "" {
		return nil
	}
	return &v.want
}

func resolveVectors() []resolveVector {
	return append(append(promotionVectors(), defaultVectors()...), append(skipVectors(), namedVectors()...)...)
}

func promotionVectors() []resolveVector {
	return []resolveVector{
		{"promote/int-long", `"int"`, `"long"`, "03", "03", true},
		{"promote/int-float", `"int"`, `"float"`, "02", "0000803f", true},
		{"promote/int-double", `"int"`, `"double"`, "01", "000000000000f0bf", true},
		{"promote/long-float-rounds", `"long"`, `"float"`, "82808010", "0000804b", true},
		{"promote/long-double", `"long"`, `"double"`, "04", "0000000000000040", true},
		{"promote/float-double", `"float"`, `"double"`, "0000c03f", "000000000000f83f", true},
		{"promote/float-double-nan", `"float"`, `"double"`, "0000c07f", "000000000000f87f", true},
		{"promote/string-bytes", `"string"`, `"bytes"`, "06666f6f", "06666f6f", true},
		{"promote/bytes-string", `"bytes"`, `"string"`, "06666f6f", "06666f6f", true},
		{"promote/array-items", `{"type":"array","items":"int"}`, `{"type":"array","items":"long"}`, "04020400", "04020400", true},
		{"promote/map-values", `{"type":"map","values":"int"}`, `{"type":"map","values":"double"}`, "0202610200", "020261000000000000f03f00", true},
		{"narrow/long-int", `"long"`, `"int"`, "02", "", false},
		{"narrow/double-float", `"double"`, `"float"`, "0000000000000040", "", false},
		{"mismatch/string-int", `"string"`, `"int"`, "0261", "", false},
		{"union/writer-union-reader-plain", `["null","int"]`, `"long"`, "0202", "02", false},
		{"union/writer-union-unreadable-branch", `["null","int"]`, `"long"`, "00", "", false},
		{"union/writer-plain-reader-union-first-match", `"int"`, `["null","long","int"]`, "02", "0202", true},
		{"union/writer-plain-reader-union-promote", `"int"`, `["null","long"]`, "02", "0202", true},
		{"union/both-unions-promote", `["null","int"]`, `["null","long"]`, "0202", "0202", true},
		{"union/both-unions-reorder", `["null","string"]`, `["string","null"]`, "020278", "000278", true},
		{"union/no-matching-branch", `"string"`, `["null","int"]`, "0278", "", false},
	}
}

func defaultVectors() []resolveVector {
	const w, a = resolveWriterRecord, "02"
	return []resolveVector{
		{"default/int", w, readerWithField(`"int"`, `7`), a, a + "0e", true},
		{"default/long", w, readerWithField(`"long"`, `-1`), a, a + "01", true},
		{"default/float", w, readerWithField(`"float"`, `1.5`), a, a + "0000c03f", true},
		{"default/double", w, readerWithField(`"double"`, `-2`), a, a + "00000000000000c0", true},
		{"default/boolean", w, readerWithField(`"boolean"`, `true`), a, a + "01", true},
		{"default/null", w, readerWithField(`"null"`, `null`), a, a, true},
		{"default/string", w, readerWithField(`"string"`, `"x"`), a, a + "0278", true},
		{"default/bytes", w, readerWithField(`"bytes"`, `"ÿ"`), a, a + "02ff", true},
		{"default/fixed", w, readerWithField(`{"type":"fixed","name":"F","size":2}`, `"\u0001\u0002"`), a, a + "0102", true},
		{"default/enum", w, readerWithField(resolveEnumABC, `"B"`), a, a + "02", true},
		{"default/array", w, readerWithField(`{"type":"array","items":"int"}`, `[1,2]`), a, a + "04020400", true},
		{"default/map", w, readerWithField(`{"type":"map","values":"int"}`, `{"k":1}`), a, a + "02026b0200", true},
		{"default/record", w, readerWithField(`{"type":"record","name":"S","fields":[{"name":"y","type":"string"}]}`, `{"y":"z"}`), a, a + "027a", true},
		{"default/union-null-first", w, readerWithField(`["null","int"]`, `null`), a, a + "00", true},
		{"default/union-second-branch", w, readerWithField(`["int","null"]`, `null`), a, a + "02", true},
		{"default/union-int-first", w, readerWithField(`["int","null"]`, `3`), a, a + "0006", true},
		{"default/decimal", w, readerWithField(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, `"\u0004Ò"`), a, a + "0404d2", true},
		{"default/missing", w, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"x","type":"int"}]}`, a, "", false},
	}
}

func skipVectors() []resolveVector {
	const r = resolveWriterRecord
	return []resolveVector{
		{
			"skip/writer-field-reader-default",
			resolveWriterRecord,
			`{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"map","values":"int"},"default":{"k":1}}]}`,
			"02", "02026b0200", true,
		},
		{"skip/string", writerWithField(`"string"`), r, "06666f6f" + "02", "02", true},
		{"skip/double", writerWithField(`"double"`), r, "000000000000f03f" + "02", "02", true},
		{"skip/fixed", writerWithField(`{"type":"fixed","name":"F","size":2}`), r, "0102" + "02", "02", true},
		{"skip/enum", writerWithField(resolveEnumABC), r, "04" + "02", "02", true},
		{"skip/array", writerWithField(`{"type":"array","items":"string"}`), r, "04027802790002", "02", true},
		{"skip/array-sized-block", writerWithField(`{"type":"array","items":"string"}`), r, "0308027802790002", "02", true},
		{"skip/map", writerWithField(`{"type":"map","values":"long"}`), r, "020261020002", "02", true},
		{"skip/union", writerWithField(`["null","string"]`), r, "020278" + "02", "02", true},
		{"skip/record", writerWithField(`{"type":"record","name":"S","fields":[{"name":"y","type":"string"},{"name":"z","type":"long"}]}`), r, "02790a" + "02", "02", true},
		{
			"reorder/fields",
			`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"b","type":"string"},{"name":"a","type":"long"}]}`,
			"020278", "027802", true,
		},
	}
}

func namedVectors() []resolveVector {
	return []resolveVector{
		{"alias/field", resolveWriterRecord, `{"type":"record","name":"R","fields":[{"name":"b","type":"int","aliases":["a"]}]}`, "02", "02", true},
		{"alias/record", resolveWriterRecord, `{"type":"record","name":"S","aliases":["R"],"fields":[{"name":"a","type":"int"}]}`, "02", "02", true},
		{"name/record-mismatch", resolveWriterRecord, `{"type":"record","name":"S","fields":[{"name":"a","type":"int"}]}`, "02", "", false},
		{"name/fixed-size-mismatch", `{"type":"fixed","name":"F","size":2}`, `{"type":"fixed","name":"F","size":3}`, "0102", "", false},
		{"enum/reader-adds-symbol", `{"type":"enum","name":"E","symbols":["A","C"]}`, resolveEnumABC, "02", "04", true},
		{"enum/unknown-symbol-default", resolveEnumABC, `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`, "04", "00", true},
		{"enum/known-symbol-with-default", resolveEnumABC, `{"type":"enum","name":"E","symbols":["B","A"],"default":"A"}`, "02", "00", true},
		{"enum/unknown-symbol-no-default", resolveEnumABC, `{"type":"enum","name":"E","symbols":["A","B"]}`, "04", "", false},
		{"enum/known-symbol-no-default", resolveEnumABC, `{"type":"enum","name":"E","symbols":["A","B"]}`, "02", "02", false},
	}
}

func TestConformance_SchemaResolution(t *testing.T) {
	xf := loadExpectedFailures(t)
	for _, v := range resolveVectors() {
		t.Run(v.name, func(t *testing.T) {
			xf.Run(t, "resolve/"+v.name, func(t xfail.T) { assertResolveVector(t, v) })
			xf.Run(t, "compat/"+v.name, func(t xfail.T) { assertCompatibility(t, v) })
		})
	}
}

func assertResolveVector(t xfail.T, v resolveVector) {
	t.Helper()

	reader, writer := parseResolvePair(t, v)
	data, err := hex.DecodeString(v.hex)
	require.NoError(t, err)

	// A fresh API keeps codecs cached by other vectors out of this check.
	api := avro.Config{DisableBlockSizeHeader: true}.Freeze()
	got, err := readResolved(api, reader, writer, data)
	if v.want == "" {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	assertReaderValue(t, api, reader, got, v.want)
}

// assertReaderValue checks a resolved value encodes with the reader schema as want and reads back unchanged.
func assertReaderValue(t xfail.T, api avro.API, reader avro.Schema, got any, want string) {
	t.Helper()

	encoded, err := api.Marshal(reader, got)
	require.NoError(t, err)
	assert.Equal(t, want, hex.EncodeToString(encoded))

	var readBack any
	require.NoError(t, api.Unmarshal(reader, encoded, &readBack))
	assert.True(t, fuzzcheck.Same(readBack, got), "resolved value %#v, reader value %#v", got, readBack)
}

// readResolved decodes writer-encoded data into a generic value through the resolved schema.
func readResolved(api avro.API, reader, writer avro.Schema, data []byte) (any, error) {
	resolved, err := avro.NewSchemaCompatibility().Resolve(reader, writer)
	if err != nil {
		return nil, err
	}
	var generic any
	err = api.Unmarshal(resolved, data, &generic)
	return generic, err
}

// resolveCacheVectors are resolved in order through one API; each must decode as if resolved alone.
func resolveCacheVectors() []resolveVector {
	var vs []resolveVector
	for _, v := range resolveVectors() {
		if v.name == "default/map" || v.name == "skip/writer-field-reader-default" {
			vs = append(vs, v)
		}
	}
	return vs
}

const resolveCacheID = "resolve-cache/default-then-skip"

func TestConformance_ResolvedCodecCache(t *testing.T) {
	loadExpectedFailures(t).Run(t, resolveCacheID, func(t xfail.T) {
		api := avro.Config{DisableBlockSizeHeader: true}.Freeze()
		for _, v := range resolveCacheVectors() {
			reader, writer := parseResolvePair(t, v)
			data, err := hex.DecodeString(v.hex)
			require.NoError(t, err)

			got, err := readResolved(api, reader, writer, data)
			require.NoError(t, err, v.name)
			assertReaderValue(t, api, reader, got, v.want)
		}
	})
}

func assertCompatibility(t xfail.T, v resolveVector) {
	t.Helper()

	reader, writer := parseResolvePair(t, v)
	err := avro.NewSchemaCompatibility().Compatible(reader, writer)
	assert.Equal(t, v.compatible, err == nil, "compatible: %v", err)
}

func parseResolvePair(t xfail.T, v resolveVector) (reader, writer avro.Schema) {
	t.Helper()

	writer, err := avro.ParseWithCache(v.writer, "", &avro.SchemaCache{})
	require.NoError(t, err)
	reader, err = avro.ParseWithCache(v.reader, "", &avro.SchemaCache{})
	require.NoError(t, err)
	return reader, writer
}
