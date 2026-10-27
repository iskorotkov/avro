// Package goccy adapts github.com/goccy/go-json to the benchmark suite.
package goccy

import (
	"io"

	"github.com/goccy/go-json"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "goccy"

// Format binds any dataset through the json struct tags of its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	return &format.Subject{
		Codec: codec{},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return json.NewEncoder(w) },
			NewDecoder: func(r io.Reader) format.Decoder { return json.NewDecoder(r) },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct{}

func (codec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (codec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
