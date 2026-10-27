// Package msgp adapts github.com/tinylib/msgp code-generated MessagePack methods to the benchmark suite.
package msgp

import (
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/tinylib/msgp/msgp"
)

// Name is the format name.
const Name = "msgp"

// Format binds datasets that have a generated mirror type in this package.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	switch ds.Name() {
	case superhero.Name:
		return bind(ds, fromSuperhero, func() any { return new(Superhero) })
	case nyctaxi.Name:
		return bind(ds, fromTrip, func() any { return new(Trip) })
	default:
		return nil, format.ErrUnsupported
	}
}

func bind[T, M any](ds dataset.Dataset, convert func(*T) M, newValue func() any) (*format.Subject, error) {
	records, err := dataset.Typed[T](ds)
	if err != nil {
		return nil, err
	}

	values := make([]any, len(records))
	for i, r := range records {
		values[i] = convert(r)
	}

	return &format.Subject{
		Codec: codec{},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return &encoder{w: msgp.NewWriter(w)} },
			NewDecoder: func(r io.Reader) format.Decoder { return decoder{r: msgp.NewReader(r)} },
		},
		Values: values,
		New:    newValue,
	}, nil
}

type codec struct{}

func (codec) Marshal(v any) ([]byte, error) { return v.(msgp.Marshaler).MarshalMsg(nil) }

func (codec) Unmarshal(data []byte, v any) error {
	_, err := v.(msgp.Unmarshaler).UnmarshalMsg(data)

	return err
}

type encoder struct{ w *msgp.Writer }

// Encode flushes per value because StreamBatch never closes its encoder.
func (e *encoder) Encode(v any) error {
	if err := v.(msgp.Encodable).EncodeMsg(e.w); err != nil {
		return err
	}

	return e.w.Flush()
}

type decoder struct{ r *msgp.Reader }

func (d decoder) Decode(v any) error { return v.(msgp.Decodable).DecodeMsg(d.r) }
