package javarun_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iskorotkov/avro/bench/report/javarun"
)

func TestRunPassesOutputAndArgs(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "run.sh")
	body := "#!/bin/sh\necho \"$@\" > \"$2\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil { //nolint:gosec // Test script must be executable.
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.json")

	if err := javarun.Run(context.Background(), script, out, []string{"-f", "1"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out) //nolint:gosec // Test temp file.
	if err != nil {
		t.Fatal(err)
	}
	if want := "-o " + out + " -f 1"; strings.TrimSpace(string(got)) != want {
		t.Errorf("script args = %q, want %q", got, want)
	}
}

func TestRunReportsFailure(t *testing.T) {
	script := filepath.Join(t.TempDir(), "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil { //nolint:gosec // Test script must be executable.
		t.Fatal(err)
	}
	if err := javarun.Run(context.Background(), script, "x.json", nil, &bytes.Buffer{}); err == nil {
		t.Error("want error for failing script")
	}
}
