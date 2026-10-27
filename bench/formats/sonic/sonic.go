//go:build (amd64 || arm64) && !go1.28

// Package sonic adapts github.com/bytedance/sonic to the benchmark suite; it is skipped on other architectures.
package sonic

import (
	"io"

	"github.com/bytedance/sonic"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "sonic"

// Format binds any dataset through the json struct tags of its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	api := sonic.ConfigDefault

	return &format.Subject{
		Codec: codec{api: api},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return api.NewEncoder(w) },
			NewDecoder: func(r io.Reader) format.Decoder { return api.NewDecoder(r) },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct{ api sonic.API }

func (c codec) Marshal(v any) ([]byte, error) { return c.api.Marshal(v) }

func (c codec) Unmarshal(data []byte, v any) error { return c.api.Unmarshal(data, v) }
