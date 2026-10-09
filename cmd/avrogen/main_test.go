package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/iskorotkov/avro/v2/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func TestAvroGen_RequiredFlags(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantExitCode int
	}{
		{
			name:         "validates schema is set",
			args:         []string{"avrogen", "-pkg", "test", "-o", "some/file"},
			wantExitCode: 1,
		},
		{
			name:         "validates schema exists",
			args:         []string{"avrogen", "-pkg", "test", "-o", "some/file", "some/schema"},
			wantExitCode: 2,
		},
		{
			name:         "validates package is set",
			args:         []string{"avrogen", "-o", "some/file", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag format are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "snake", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag key are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", ":snake", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag style are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "json:something", "schema.avsc"},
			wantExitCode: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := realMain(test.args, io.Discard, io.Discard)

			assert.Equal(t, test.wantExitCode, got)
		})
	}
}

func TestAvroGen_GeneratesSchemaStdout(t *testing.T) {
	var buf bytes.Buffer

	args := []string{"avrogen", "-pkg", "testpkg", "-pkgdoc", "package testpkg is generated from schema.avsc", "testdata/schema.avsc"}
	gotCode := realMain(args, &buf, io.Discard)
	require.Equal(t, 0, gotCode)

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, buf.Bytes())
}

func TestAvroGen_GeneratesSchema(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{"avrogen", "-o", file, "-pkg", "testpkg", "-pkgdoc", "package testpkg is generated from schema.avsc", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFullname(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-fullname", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_fullname.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fullname.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithEncoders(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-encoders", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_encoders.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFullSchema(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-encoders", "-fullschema", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_encoders_fullschema.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders_fullschema.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithStrictTypes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-strict-types", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_stricttypes.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_stricttypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithCustomLogicalTypes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{
		"avrogen", "-pkg", "testpkg", "-o", file,
		// test mapping to an external package type
		"-logicaltype", "uuid,uuid.UUID,github.com/google/uuid",
		// test mapping to a stdlib package type
		"-logicaltype", "decimal,json.RawMessage,encoding/json",
		// test mapping to a built-in type
		"-logicaltype", "date,int32",
		"testdata/schema_logicaltypes.avsc",
	}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_logicaltypes.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_logicaltypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFieldTypes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	args := []string{
		"avrogen", "-pkg", "testpkg", "-o", file,
		// test mapping to a stdlib package type
		"-fieldtype", "a.b.test.someBytes,json.RawMessage,encoding/json",
		"testdata/schema_fieldtypes.avsc",
	}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_fieldtypes.go", got, 0o600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fieldtypes.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_UnmatchedFieldTypeSelectorFails(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.go")
	var stderr bytes.Buffer
	args := []string{
		"avrogen", "-pkg", "testpkg", "-o", file,
		"-fieldtype", "a.b.test.doesNotExist,json.RawMessage,encoding/json",
		"testdata/schema_fieldtypes.avsc",
	}
	gotCode := realMain(args, io.Discard, &stderr)

	require.Equal(t, 5, gotCode)
	assert.Contains(t, stderr.String(),
		"field type overrides matched no field: a.b.test.doesNotExist")
}

func TestParseLogicalType(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    gen.LogicalType
		errFunc assert.ErrorAssertionFunc
	}{
		{
			name:    "built-in type with no import",
			raw:     "date,int32",
			want:    gen.LogicalType{Name: "date", Typ: "int32"},
			errFunc: assert.NoError,
		},
		{
			name: "stdlib import",
			raw:  "decimal,json.RawMessage,encoding/json",
			want: gen.LogicalType{
				Name: "decimal", Typ: "json.RawMessage", Import: "encoding/json",
			},
			errFunc: assert.NoError,
		},
		{
			name: "third-party import",
			raw:  "uuid,uuid.UUID,github.com/google/uuid",
			want: gen.LogicalType{
				Name: "uuid", Typ: "uuid.UUID", ThirdPartyImport: "github.com/google/uuid",
			},
			errFunc: assert.NoError,
		},
		{
			name:    "missing comma",
			raw:     "uuid",
			errFunc: assert.Error,
		},
		{
			name:    "too many parts",
			raw:     "uuid,uuid.UUID,github.com/google/uuid,extra",
			errFunc: assert.Error,
		},
		{
			name:    "empty name",
			raw:     ",uuid.UUID,github.com/google/uuid",
			errFunc: assert.Error,
		},
		{
			name:    "empty type",
			raw:     "uuid,,github.com/google/uuid",
			errFunc: assert.Error,
		},
		{
			name:    "generic type with no import",
			raw:     "uuid,pkg.Pair[string,int]",
			want:    gen.LogicalType{Name: "uuid", Typ: "pkg.Pair[string,int]"},
			errFunc: assert.NoError,
		},
		{
			name: "generic type with import",
			raw:  "uuid,pkg.Pair[string,int],example.com/pkg",
			want: gen.LogicalType{
				Name: "uuid", Typ: "pkg.Pair[string,int]", ThirdPartyImport: "example.com/pkg",
			},
			errFunc: assert.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseLogicalType(test.raw)

			test.errFunc(t, err)
			if err == nil {
				assert.Equal(t, test.want, got)
			}
		})
	}
}

func TestParseFieldType(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    gen.FieldType
		errFunc assert.ErrorAssertionFunc
	}{
		{
			name:    "built-in type with no import",
			raw:     "a.b.test.someBytes,[]byte",
			want:    gen.FieldType{Record: "a.b.test", Field: "someBytes", Typ: "[]byte"},
			errFunc: assert.NoError,
		},
		{
			name: "stdlib import",
			raw:  "a.b.test.someBytes,json.RawMessage,encoding/json",
			want: gen.FieldType{
				Record: "a.b.test", Field: "someBytes",
				Typ: "json.RawMessage", Import: "encoding/json",
			},
			errFunc: assert.NoError,
		},
		{
			name: "third-party import",
			raw:  "a.b.test.id,uuid.UUID,github.com/google/uuid",
			want: gen.FieldType{
				Record: "a.b.test", Field: "id",
				Typ: "uuid.UUID", ThirdPartyImport: "github.com/google/uuid",
			},
			errFunc: assert.NoError,
		},
		{
			name:    "missing comma",
			raw:     "a.b.test.someBytes",
			errFunc: assert.Error,
		},
		{
			name:    "too many parts",
			raw:     "a.b.test.someBytes,json.RawMessage,encoding/json,extra",
			errFunc: assert.Error,
		},
		{
			name:    "selector without dot",
			raw:     "someBytes,json.RawMessage,encoding/json",
			errFunc: assert.Error,
		},
		{
			name:    "empty type",
			raw:     "a.b.test.someBytes,,encoding/json",
			errFunc: assert.Error,
		},
		{
			name:    "generic type with no import",
			raw:     "a.b.test.someBytes,pkg.Pair[string,int]",
			want:    gen.FieldType{Record: "a.b.test", Field: "someBytes", Typ: "pkg.Pair[string,int]"},
			errFunc: assert.NoError,
		},
		{
			name: "generic type with import",
			raw:  "a.b.test.someBytes,pkg.Pair[string,int],example.com/pkg",
			want: gen.FieldType{
				Record: "a.b.test", Field: "someBytes",
				Typ: "pkg.Pair[string,int]", ThirdPartyImport: "example.com/pkg",
			},
			errFunc: assert.NoError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseFieldType(test.raw)

			test.errFunc(t, err)
			if err == nil {
				assert.Equal(t, test.want, got)
			}
		})
	}
}

func TestParseTags(t *testing.T) {
	tests := []struct {
		name string
		tags string
	}{
		{
			name: "snake case",
			tags: "json:snake",
		},
		{
			name: "camel case",
			tags: "json:camel",
		},
		{
			name: "upper camel case",
			tags: "json:upper-camel",
		},
		{
			name: "kebab case",
			tags: "json:kebab",
		},
		{
			name: "original case",
			tags: "json:original",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseTags(test.tags)

			require.NoError(t, err)
		})
	}
}

func TestParseInitialisms(t *testing.T) {
	tests := []struct {
		name        string
		initialisms string
		errFunc     assert.ErrorAssertionFunc
	}{
		{
			name:        "single initialism",
			initialisms: "ABC",
			errFunc:     assert.NoError,
		},
		{
			name:        "multiple initialisms",
			initialisms: "ABC,DEF",
			errFunc:     assert.NoError,
		},
		{
			name:        "wrong initialism",
			initialisms: "ABC,def,GHI",
			errFunc:     assert.Error,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseInitialisms(test.initialisms)

			test.errFunc(t, err)
		})
	}
}
