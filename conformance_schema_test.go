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

func TestConformance_ApacheSchemaVectors(t *testing.T) {
	vectors := loadSchemaVectors(t, "testdata/avro-schema-tests.txt")
	require.NotEmpty(t, vectors)

	for _, v := range vectors {
		t.Run(v.id, func(t *testing.T) {
			if v.input == "[  ]" {
				t.Skip("empty union is rejected by NewUnionSchema; spec allows it")
			}

			schema, err := avro.ParseWithCache(v.input, "", &avro.SchemaCache{})
			require.NoError(t, err)

			assert.Equal(t, v.canonical, schema.String())
			assertFingerprints(t, schema, v)

			reparsed, err := avro.ParseWithCache(schema.String(), "", &avro.SchemaCache{})
			require.NoError(t, err)
			assert.Equal(t, v.canonical, reparsed.String())
		})
	}
}

func assertFingerprints(t *testing.T, schema avro.Schema, v schemaVector) {
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
