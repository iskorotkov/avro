// Package jsonexp adapts github.com/go-json-experiment/json (the encoding/json/v2 prototype) to the benchmark suite.
package jsonexp

import (
	"io"

	"github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "json-exp"

// Format binds any dataset through the json struct tags of its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	return &format.Subject{
		Codec: codec{},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return encoder{jsontext.NewEncoder(w)} },
			NewDecoder: func(r io.Reader) format.Decoder { return decoder{jsontext.NewDecoder(r)} },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct{}

func (codec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (codec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type encoder struct{ enc *jsontext.Encoder }

func (e encoder) Encode(v any) error { return json.MarshalEncode(e.enc, v) }

type decoder struct{ dec *jsontext.Decoder }

func (d decoder) Decode(v any) error { return json.UnmarshalDecode(d.dec, v) }
