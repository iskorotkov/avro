// Package suite binds every format to every dataset and defines the benchmarked operations.
package suite

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Case is a format bound to a dataset.
type Case struct {
	Dataset string
	Format  string
	Subject *format.Subject
}

// Load loads every dataset, from full data under dataDir when set and capped at rows records when positive, and binds every format to it.
func Load(dataDir string, rows int, providers []dataset.Provider, formats []format.Format) ([]Case, error) {
	var cases []Case
	for _, p := range providers {
		ds, err := p.Load(fullDir(dataDir, p), rows)
		if err != nil {
			return nil, err
		}

		for _, f := range formats {
			s, err := f.Bind(ds)
			if errors.Is(err, format.ErrUnsupported) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("bind %s to %s: %w", f.Name(), ds.Name(), err)
			}
			cases = append(cases, Case{Dataset: ds.Name(), Format: f.Name(), Subject: s})
		}
	}

	return cases, nil
}

func fullDir(dataDir string, p dataset.Provider) string {
	if dataDir == "" || len(p.Sources()) == 0 {
		return ""
	}

	return filepath.Join(dataDir, p.Name())
}

// Rotate returns formats rotated left by n so that each round starts with a different format.
func Rotate(formats []format.Format, n int) []format.Format {
	if len(formats) == 0 {
		return formats
	}

	n %= len(formats)

	return slices.Concat(formats[n:], formats[:n])
}
