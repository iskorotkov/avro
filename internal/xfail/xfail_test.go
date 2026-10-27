package xfail_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/iskorotkov/avro/v2/internal/xfail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReporter records what Run reports.
type fakeReporter struct {
	errors  []string
	logs    []string
	stopped bool
}

func (f *fakeReporter) Helper() {}

func (f *fakeReporter) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}

func (f *fakeReporter) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

func (f *fakeReporter) FailNow() { f.stopped = true }

var listed = xfail.List{"known": {ID: "known", Reason: "bug", Tracking: "issue 1"}}

func pass(xfail.T) {}

func failSoft(t xfail.T) { t.Errorf("mismatch %d", 1) }

func failHard(t xfail.T) {
	t.Errorf("first")
	t.FailNow()
	t.Errorf("unreachable")
}

func TestList_Run(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		check       func(xfail.T)
		wantErrors  []string
		wantLogs    []string
		wantStopped bool
	}{
		{name: "unlisted pass", id: "other", check: pass},
		{name: "unlisted soft failure", id: "other", check: failSoft, wantErrors: []string{"mismatch 1"}},
		{name: "unlisted hard failure", id: "other", check: failHard, wantErrors: []string{"first"}, wantStopped: true},
		{name: "unlisted panic", id: "other", check: func(xfail.T) { panic("boom") }, wantErrors: []string{"panic: boom"}},
		{name: "listed failure", id: "known", check: failHard, wantLogs: []string{"expected failure (issue 1): bug"}},
		{
			name: "listed pass", id: "known", check: pass,
			wantErrors: []string{"known passes now; remove it from the expected failures (issue 1)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var r fakeReporter
			listed.Run(&r, test.id, test.check)

			assert.Equal(t, test.wantErrors, r.errors)
			assert.Equal(t, test.wantLogs, r.logs)
			assert.Equal(t, test.wantStopped, r.stopped)
		})
	}
}

func TestList_Unknown(t *testing.T) {
	l := xfail.List{"b": {}, "a": {}, "c": {}}

	assert.Equal(t, []string{"a", "c"}, l.Unknown([]string{"b", "d"}))
	assert.Empty(t, l.Unknown([]string{"a", "b", "c"}))
}

func TestLoad(t *testing.T) {
	path := writeFile(t, `[{"id":"a","reason":"r","tracking":"t"}]`)

	got, err := xfail.Load(path)

	require.NoError(t, err)
	assert.Equal(t, xfail.List{"a": {ID: "a", Reason: "r", Tracking: "t"}}, got)
}

func TestLoad_Errors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "invalid json", content: `{`, wantErr: "xfail: parse"},
		{name: "missing reason", content: `[{"id":"a","tracking":"t"}]`, wantErr: `entry "a" needs an id, a reason and a tracking note`},
		{name: "missing tracking", content: `[{"id":"a","reason":"r"}]`, wantErr: `entry "a" needs an id, a reason and a tracking note`},
		{
			name:    "duplicate",
			content: `[{"id":"a","reason":"r","tracking":"t"},{"id":"a","reason":"r","tracking":"t"}]`,
			wantErr: `xfail: duplicate entry "a"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := xfail.Load(writeFile(t, test.content))

			assert.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := xfail.Load(filepath.Join(t.TempDir(), "missing.json"))

	assert.ErrorIs(t, err, os.ErrNotExist)
}

func writeFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "xfail.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}
