//go:build !((amd64 || arm64) && !go1.28)

// Package sonic adapts github.com/bytedance/sonic to the benchmark suite; it is skipped on other architectures.
package sonic

import (
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "sonic"

// Format is unsupported on this architecture.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(dataset.Dataset) (*format.Subject, error) { return nil, format.ErrUnsupported }
