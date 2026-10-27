//go:build javadiff

package avro_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/require"
)

// TestJavaDifferential checks every conformance case against the Java harness jar set in JAVADIFF_JAR.
func TestJavaDifferential(t *testing.T) {
	jar := os.Getenv("JAVADIFF_JAR")
	require.NotEmpty(t, jar, "JAVADIFF_JAR must point at the jar built from testdata/javadiff")

	dir := t.TempDir()
	cases := javaCases(t, dir)
	prepErrs := make(map[string]error)
	for i := range cases {
		if cases[i].prepare != nil {
			prepErrs[cases[i].ID] = cases[i].prepare(&cases[i])
		}
	}
	results := runJavaHarness(t, jar, dir, cases)

	xf := loadExpectedFailures(t)
	for _, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			xf.Run(t, c.ID, func(t xfail.T) {
				require.NoError(t, prepErrs[c.ID], "prepare go input")
				got, ok := results[c.ID]
				require.True(t, ok, "java reported no result")
				c.check(t, got)
			})
		})
	}
}

func runJavaHarness(t *testing.T, jar, dir string, cases []javaCase) map[string]javaResult {
	t.Helper()

	manifest, err := json.Marshal(map[string]any{"cases": cases})
	require.NoError(t, err)
	in, out := filepath.Join(dir, "manifest.json"), filepath.Join(dir, "results.json")
	require.NoError(t, os.WriteFile(in, manifest, 0o600))

	cmd := exec.CommandContext(t.Context(), "java", "-jar", jar, in, out) //nolint:gosec
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "java harness: %s", stderr.String())

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	var results map[string]javaResult
	require.NoError(t, json.Unmarshal(data, &results))
	return results
}
