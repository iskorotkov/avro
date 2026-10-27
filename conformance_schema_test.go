package avro_test

import (
	"bufio"
	"crypto/md5" //nolint:gosec
	"crypto/sha256"
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaVector is a single case from the Apache Avro schema-tests.txt file.
type schemaVector struct {
	id          string
	input       string
	canonical   string
	fingerprint *int64
}

// extraSchemaVectors complements the Apache vectors; an empty canonical form means Java rejects the schema.
func extraSchemaVectors() []schemaVector {
	return []schemaVector{
		{id: "invalid/record-duplicate-field", input: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"long"}]}`},
		{id: "invalid/record-missing-fields", input: `{"type":"record","name":"R"}`},
		{id: "invalid/record-missing-name", input: `{"type":"record","fields":[]}`},
		{id: "invalid/field-missing-type", input: `{"type":"record","name":"R","fields":[{"name":"a"}]}`},
		{id: "invalid/attribute-name-case", input: `{"type":"record","NAME":"R","fields":[]}`},
		{id: "invalid/attribute-size-case", input: `{"type":"fixed","name":"F","SIZE":1}`},
		{id: "invalid/name-leading-digit", input: `{"type":"fixed","name":"1F","size":1}`},
		{id: "invalid/namespace-empty-part", input: `{"type":"fixed","name":"F","namespace":"a..b","size":1}`},
		{id: "invalid/name-redefined", input: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":1}},{"name":"b","type":{"type":"fixed","name":"F","size":2}}]}`},
		{id: "invalid/enum-duplicate-symbol", input: `{"type":"enum","name":"E","symbols":["A","A"]}`},
		{id: "invalid/enum-invalid-symbol", input: `{"type":"enum","name":"E","symbols":["1A"]}`},
		{id: "invalid/enum-default-not-symbol", input: `{"type":"enum","name":"E","symbols":["A"],"default":"B"}`},
		{id: "invalid/fixed-missing-size", input: `{"type":"fixed","name":"F"}`},
		{id: "invalid/fixed-negative-size", input: `{"type":"fixed","name":"F","size":-1}`},
		{id: "invalid/array-missing-items", input: `{"type":"array"}`},
		{id: "invalid/map-missing-values", input: `{"type":"map"}`},
		{id: "invalid/unknown-type", input: `"foo"`},
		{id: "invalid/union-duplicate-primitive", input: `["int","int"]`},
		{id: "invalid/union-duplicate-array", input: `[{"type":"array","items":"int"},{"type":"array","items":"long"}]`},
		{id: "invalid/union-nested", input: `["null",["int"]]`},
		{id: "invalid/default-int-from-string", input: `{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":"x"}]}`},
		{id: "invalid/default-null-for-int", input: `{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":null}]}`},
		{id: "invalid/default-enum-not-symbol", input: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"enum","name":"E","symbols":["A"]},"default":"Z"}]}`},
		{id: "invalid/default-record-missing-field", input: `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]},"default":{}}]}`},
		{
			id:        "valid/default-union-second-branch",
			input:     `{"type":"record","name":"R","fields":[{"name":"a","type":["int","null"],"default":null}]}`,
			canonical: `{"name":"R","type":"record","fields":[{"name":"a","type":["int","null"]}]}`,
		},
		{
			id:        "lenient/default-fixed-wrong-size",
			input:     `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":2},"default":"\u0001"}]}`,
			canonical: `{"name":"R","type":"record","fields":[{"name":"a","type":{"name":"F","type":"fixed","size":2}}]}`,
		},
		{id: "logical/date", input: `{"type":"int","logicalType":"date"}`, canonical: `"int"`},
		{id: "logical/timestamp-nanos", input: `{"type":"long","logicalType":"timestamp-nanos"}`, canonical: `"long"`},
		{id: "logical/uuid", input: `{"type":"string","logicalType":"uuid"}`, canonical: `"string"`},
		{id: "logical/decimal-bytes", input: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, canonical: `"bytes"`},
		{id: "logical/decimal-fixed", input: `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":9}`, canonical: `{"name":"D","type":"fixed","size":4}`},
		{id: "logical/duration", input: `{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`, canonical: `{"name":"Dur","type":"fixed","size":12}`},
		{id: "logical/decimal-scale-exceeds-precision", input: `{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}`, canonical: `"bytes"`},
		{id: "logical/decimal-zero-precision", input: `{"type":"bytes","logicalType":"decimal","precision":0}`, canonical: `"bytes"`},
		{id: "logical/decimal-precision-exceeds-fixed", input: `{"type":"fixed","name":"D","size":1,"logicalType":"decimal","precision":5}`, canonical: `{"name":"D","type":"fixed","size":1}`},
		{id: "logical/date-on-long", input: `{"type":"long","logicalType":"date"}`, canonical: `"long"`},
		{id: "logical/timestamp-millis-on-int", input: `{"type":"int","logicalType":"timestamp-millis"}`, canonical: `"int"`},
		{id: "logical/uuid-on-int", input: `{"type":"int","logicalType":"uuid"}`, canonical: `"int"`},
		{id: "logical/duration-wrong-size", input: `{"type":"fixed","name":"Dur","size":11,"logicalType":"duration"}`, canonical: `{"name":"Dur","type":"fixed","size":11}`},
		{id: "logical/unknown", input: `{"type":"string","logicalType":"foo"}`, canonical: `"string"`},
	}
}

// allSchemaVectors returns the Apache vectors under "apache/" followed by the extra vectors.
func allSchemaVectors(t *testing.T) []schemaVector {
	t.Helper()

	vectors := loadSchemaVectors(t, "testdata/avro-schema-tests.txt")
	require.NotEmpty(t, vectors)
	for i := range vectors {
		vectors[i].id = "apache/" + vectors[i].id
	}
	return append(vectors, extraSchemaVectors()...)
}

func TestConformance_SchemaVectors(t *testing.T) {
	xf := loadExpectedFailures(t)
	for _, v := range allSchemaVectors(t) {
		t.Run(v.id, func(t *testing.T) {
			xf.Run(t, "schema/"+v.id, func(t xfail.T) { assertSchemaVector(t, v) })
		})
	}
}

func assertSchemaVector(t xfail.T, v schemaVector) {
	t.Helper()

	schema, err := avro.ParseWithCache(v.input, "", &avro.SchemaCache{})
	if v.canonical == "" {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)

	assert.Equal(t, v.canonical, schema.String())
	assertFingerprints(t, schema, v)

	reparsed, err := avro.ParseWithCache(schema.String(), "", &avro.SchemaCache{})
	require.NoError(t, err)
	assert.Equal(t, v.canonical, reparsed.String())
}

func assertFingerprints(t xfail.T, schema avro.Schema, v schemaVector) {
	t.Helper()

	md5Want := md5.Sum([]byte(v.canonical)) //nolint:gosec
	sha256Want := sha256.Sum256([]byte(v.canonical))

	md5Got, err := schema.FingerprintUsing(avro.MD5)
	require.NoError(t, err)
	assert.Equal(t, md5Want[:], md5Got)

	sha256Got, err := schema.FingerprintUsing(avro.SHA256)
	require.NoError(t, err)
	assert.Equal(t, sha256Want[:], sha256Got)
	assert.Equal(t, sha256Want, schema.Fingerprint())

	if v.fingerprint == nil {
		return
	}
	want := uint64(*v.fingerprint) //nolint:gosec

	crcBE, err := schema.FingerprintUsing(avro.CRC64Avro)
	require.NoError(t, err)
	assert.Equal(t, want, binary.BigEndian.Uint64(crcBE))

	crcLE, err := schema.FingerprintUsing(avro.CRC64AvroLE)
	require.NoError(t, err)
	assert.Equal(t, want, binary.LittleEndian.Uint64(crcLE))
}

// loadSchemaVectors parses the Apache Avro "<<INPUT/<<canonical/<<fingerprint" vector format.
func loadSchemaVectors(t *testing.T, path string) []schemaVector {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var (
		vectors []schemaVector
		cur     *schemaVector
		inBlock bool
		block   []string
	)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case inBlock && line == "INPUT":
			cur.input = strings.Join(block, "\n")
			inBlock, block = false, nil
		case inBlock:
			block = append(block, line)
		case strings.HasPrefix(line, "// ") && isVectorID(line[3:]):
			vectors = append(vectors, schemaVector{id: line[3:]})
			cur = &vectors[len(vectors)-1]
		case line == "<<INPUT":
			inBlock = true
		case strings.HasPrefix(line, "<<INPUT "):
			cur.input = strings.TrimPrefix(line, "<<INPUT ")
		case strings.HasPrefix(line, "<<canonical "):
			cur.canonical = strings.TrimPrefix(line, "<<canonical ")
		case strings.HasPrefix(line, "<<fingerprint "):
			fp, err := strconv.ParseInt(strings.TrimPrefix(line, "<<fingerprint "), 10, 64)
			require.NoError(t, err)
			cur.fingerprint = &fp
		}
	}
	require.NoError(t, sc.Err())
	require.False(t, inBlock, "unterminated INPUT block")
	return vectors
}

func isVectorID(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}
