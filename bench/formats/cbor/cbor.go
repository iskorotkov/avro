// Package cbor adapts github.com/fxamacker/cbor/v2 to the benchmark suite.
package cbor

import (
	"io"

	"github.com/fxamacker/cbor/v2"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "cbor"

// Format binds any dataset through the json struct tags of its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	// Epoch-based modes lose sub-second precision, so use the exact text form.
	enc, err := cbor.EncOptions{Time: cbor.TimeRFC3339Nano}.EncMode()
	if err != nil {
		return nil, err
	}

	return &format.Subject{
		Codec: codec{enc: enc},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return enc.NewEncoder(w) },
			NewDecoder: func(r io.Reader) format.Decoder { return cbor.NewDecoder(r) },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct{ enc cbor.EncMode }

func (c codec) Marshal(v any) ([]byte, error) { return c.enc.Marshal(v) }

func (codec) Unmarshal(data []byte, v any) error { return cbor.Unmarshal(data, v) }
