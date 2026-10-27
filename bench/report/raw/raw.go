// Package raw reads benchfmt result files.
package raw

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/perf/benchfmt"
)

// Read parses benchfmt text and returns independent copies of every result.
func Read(r io.Reader, name string) ([]*benchfmt.Result, error) {
	reader := benchfmt.NewReader(r, name)
	var out []*benchfmt.Result
	for reader.Scan() {
		switch rec := reader.Result().(type) {
		case *benchfmt.Result:
			out = append(out, rec.Clone())
		case *benchfmt.SyntaxError:
			return nil, rec
		}
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return out, nil
}

// ReadFile parses the benchfmt file at path.
func ReadFile(path string) ([]*benchfmt.Result, error) {
	f, err := os.Open(path) //nolint:gosec // Reading the user-selected results file is the purpose.
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // Read-only file.
	return Read(f, path)
}
