package avro_test

import (
	"bytes"
	"crypto/md5" //nolint:gosec
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/iskorotkov/avro/v2/pkg/crc64"
	"github.com/iskorotkov/avro/v2/soe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// javaCase is one operation for the Java harness in testdata/javadiff and the check of its result.
type javaCase struct {
	ID     string `json:"id"`
	Op     string `json:"op"`
	Schema string `json:"schema,omitempty"`
	Reader string `json:"reader,omitempty"`
	Hex    string `json:"hex"`
	Codec  string `json:"codec,omitempty"`
	File   string `json:"file,omitempty"`
	Count  int    `json:"count,omitempty"`

	// prepare produces the Go-side input of the case before the harness runs.
	prepare func(c *javaCase) error
	check   func(t xfail.T, got javaResult)
}

// javaResult is what the Java harness reports for one case.
type javaResult struct {
	Error      string `json:"error"`
	Hex        string `json:"hex"`
	Codec      string `json:"codec"`
	Count      int    `json:"count"`
	Canonical  string `json:"canonical"`
	CRC64      string `json:"crc64"`
	MD5        string `json:"md5"`
	SHA256     string `json:"sha256"`
	Compatible *bool  `json:"compatible"`
}

// javaCodecs are the container codecs both implementations support.
var javaCodecs = []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy, ocf.ZStandard}

// ocfMultiBlockRecords is how many single-record blocks the multi-block container files hold.
const ocfMultiBlockRecords = 3

// javaOnlyCodecs are container codecs Java writes and this library must reject cleanly.
var javaOnlyCodecs = []string{"bzip2", "xz"}

// javaCases builds every Java case, keeping container files in dir.
func javaCases(t *testing.T, dir string) []javaCase {
	t.Helper()

	var cases []javaCase
	for _, v := range binaryVectors() {
		cases = append(cases, javaDecodeCase("java/binary/"+v.name, v.schema, "", v.hex, &v.hex))
	}
	for _, v := range invalidBinaryVectors() {
		cases = append(cases, javaDecodeCase("java/invalid/"+v.name, v.schema, "", v.hex, nil))
	}
	for _, v := range resolveVectors() {
		cases = append(cases,
			javaDecodeCase("java/resolve/"+v.name, v.writer, v.reader, v.hex, v.wantHex()),
			javaCompatCase(v),
		)
	}
	for _, v := range allSchemaVectors(t) {
		cases = append(cases, javaSchemaCase(v))
	}
	for i, v := range binaryVectors() {
		for _, codec := range javaCodecs {
			cases = append(cases,
				goToJavaOCFCase(v, v.name, codec, 1, filepath.Join(dir, fmt.Sprintf("go-%d-%s.avro", i, codec))),
				javaToGoOCFCase(v, v.name, codec, 1, filepath.Join(dir, fmt.Sprintf("java-%d-%s.avro", i, codec))),
			)
		}
		cases = append(cases, javaSOEEncodeCase(v), javaSOEDecodeCase(v))
	}
	multi := binaryVector{"record/spec", matrixRecordSchema, matrixRecord{A: 27, B: "foo"}, "3606666f6f"}
	for _, codec := range javaCodecs {
		cases = append(cases,
			goToJavaOCFCase(multi, "multi-block", codec, ocfMultiBlockRecords, filepath.Join(dir, "go-multi-"+string(codec)+".avro")),
			javaToGoOCFCase(multi, "multi-block", codec, ocfMultiBlockRecords, filepath.Join(dir, "java-multi-"+string(codec)+".avro")),
		)
	}
	for _, codec := range javaOnlyCodecs {
		cases = append(cases, javaOnlyCodecCase(codec, filepath.Join(dir, "java-only-"+codec+".avro")))
	}
	return cases
}

// javaDecodeCase decodes hex in Java; want is the expected re-encoding, nil when Java must reject the input.
func javaDecodeCase(id, writer, reader, data string, want *string) javaCase {
	c := javaCase{ID: id, Op: "decode", Schema: writer, Reader: reader, Hex: data}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		if want == nil {
			assert.NotEmpty(t, got.Error, "java accepted the input and wrote %s", got.Hex)
			return
		}
		require.Empty(t, got.Error)
		assert.Equal(t, *want, got.Hex)
	}
	return c
}

func javaCompatCase(v resolveVector) javaCase {
	c := javaCase{ID: "java/compat/" + v.name, Op: "compat", Schema: v.writer, Reader: v.reader}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		require.NotNil(t, got.Compatible)
		assert.Equal(t, v.compatible, *got.Compatible)
	}
	return c
}

func javaSchemaCase(v schemaVector) javaCase {
	c := javaCase{ID: "java/schema/" + v.id, Op: "schema", Schema: v.input}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		if v.canonical == "" {
			assert.NotEmpty(t, got.Error, "java accepted the schema as %s", got.Canonical)
			return
		}
		require.Empty(t, got.Error)
		assert.Equal(t, v.canonical, got.Canonical)

		md5Want := md5.Sum([]byte(v.canonical)) //nolint:gosec
		sha256Want := sha256.Sum256([]byte(v.canonical))
		assert.Equal(t, hex.EncodeToString(md5Want[:]), got.MD5)
		assert.Equal(t, hex.EncodeToString(sha256Want[:]), got.SHA256)

		h := crc64.New()
		_, _ = h.Write([]byte(v.canonical))
		crcWant := int64(binary.BigEndian.Uint64(h.Sum(nil))) //nolint:gosec
		assert.Equal(t, strconv.FormatInt(crcWant, 10), got.CRC64)
		if v.fingerprint != nil {
			assert.Equal(t, strconv.FormatInt(*v.fingerprint, 10), got.CRC64)
		}
	}
	return c
}

// goToJavaOCFCase writes records copies of a vector in Go, one block each, that Java reads back.
func goToJavaOCFCase(v binaryVector, name string, codec ocf.CodecName, records int, file string) javaCase {
	c := javaCase{ID: "ocf/" + string(codec) + "/go-to-java/" + name, Op: "ocf-read", File: file}
	c.prepare = func(c *javaCase) error {
		data, err := writeGoOCF(v, codec, records)
		if err != nil {
			return err
		}
		return os.WriteFile(c.File, data, 0o600)
	}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		assert.Equal(t, string(codec), got.Codec)
		assert.Equal(t, records, got.Count)
		assert.Equal(t, strings.Repeat(v.hex, records), got.Hex)
	}
	return c
}

// javaToGoOCFCase has Java write records copies of a vector, one block each, that Go reads back.
func javaToGoOCFCase(v binaryVector, name string, codec ocf.CodecName, records int, file string) javaCase {
	c := javaCase{
		ID: "ocf/" + string(codec) + "/java-to-go/" + name, Op: "ocf-write",
		Schema: v.schema, Hex: v.hex, Codec: string(codec), File: file, Count: records,
	}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		assertGoReadsOCF(t, data, v, records)
	}
	return c
}

func javaOnlyCodecCase(codec, file string) javaCase {
	v := primitiveVectors()[0]
	c := javaCase{ID: "ocf/" + codec + "/java-to-go/rejected", Op: "ocf-write", Schema: v.schema, Hex: v.hex, Codec: codec, File: file}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		_, err = ocf.NewDecoder(bytes.NewReader(data))
		assert.ErrorContains(t, err, "unknown codec")
	}
	return c
}

// javaSOEEncodeCase compares Java single-object encoding with Go and decodes it in Go.
func javaSOEEncodeCase(v binaryVector) javaCase {
	c := javaCase{ID: "soe/java-encode/" + v.name, Op: "soe-encode", Schema: v.schema, Hex: v.hex}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		codec := newSOECodec(t, v)
		want, err := codec.Encode(v.value)
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(want), got.Hex)

		msg, err := hex.DecodeString(got.Hex)
		require.NoError(t, err)
		out := reflect.New(reflect.TypeOf(v.value))
		require.NoError(t, codec.Decode(msg, out.Interface()))
		assertSameValue(t, v.value, out.Elem().Interface())
	}
	return c
}

// javaSOEDecodeCase has Java decode a Go single-object encoding.
func javaSOEDecodeCase(v binaryVector) javaCase {
	c := javaCase{ID: "soe/java-decode/" + v.name, Op: "soe-decode", Schema: v.schema}
	c.prepare = func(c *javaCase) error {
		schema, err := avro.Parse(v.schema)
		if err != nil {
			return err
		}
		codec, err := soe.NewCodecWithAPI(schema, specAPI)
		if err != nil {
			return err
		}
		msg, err := codec.Encode(v.value)
		c.Hex = hex.EncodeToString(msg)
		return err
	}
	c.check = func(t xfail.T, got javaResult) {
		t.Helper()

		require.Empty(t, got.Error)
		assert.Equal(t, v.hex, got.Hex)
	}
	return c
}

func newSOECodec(t xfail.T, v binaryVector) *soe.Codec {
	t.Helper()

	schema, err := avro.Parse(v.schema)
	require.NoError(t, err)
	codec, err := soe.NewCodecWithAPI(schema, specAPI)
	require.NoError(t, err)
	return codec
}

func writeGoOCF(v binaryVector, codec ocf.CodecName, records int) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(v.schema, &buf, ocf.WithCodec(codec), ocf.WithBlockLength(1))
	if err != nil {
		return nil, err
	}
	for range records {
		if err = enc.Encode(v.value); err != nil {
			return nil, err
		}
	}
	if err = enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// assertGoReadsOCF reads a container file of records copies of a vector into its Go type and generically.
func assertGoReadsOCF(t xfail.T, data []byte, v binaryVector, records int) {
	t.Helper()

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	for range records {
		require.True(t, dec.HasNext(), "container ended early: %v", dec.Error())
		out := reflect.New(reflect.TypeOf(v.value))
		require.NoError(t, dec.Decode(out.Interface()))
		assertSameValue(t, v.value, out.Elem().Interface())
	}
	assert.False(t, dec.HasNext())
	require.NoError(t, dec.Error())

	dec, err = ocf.NewDecoder(bytes.NewReader(data))
	require.NoError(t, err)
	for range records {
		require.True(t, dec.HasNext(), "container ended early: %v", dec.Error())
		var generic any
		require.NoError(t, dec.Decode(&generic))
		got, err := specAPI.Marshal(dec.Schema(), generic)
		require.NoError(t, err)
		assert.Equal(t, v.hex, hex.EncodeToString(got))
	}
}
