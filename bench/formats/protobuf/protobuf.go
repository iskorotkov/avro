// Package protobuf adapts google.golang.org/protobuf generated messages to the benchmark suite.
package protobuf

//go:generate go tool buf generate

import (
	"bufio"
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/earthquakes"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/datasets/wikirc"
	"github.com/iskorotkov/avro/bench/format"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

// Name is the format name.
const Name = "protobuf"

// Format binds datasets that have a generated message in this package.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	switch ds.Name() {
	case superhero.Name:
		return bind(ds, fromSuperhero)
	case nyctaxi.Name:
		return bind(ds, fromTrip)
	case earthquakes.Name:
		return bind(ds, fromQuake)
	case wikirc.Name:
		return bind(ds, fromRecentChange)
	default:
		return nil, format.ErrUnsupported
	}
}

func bind[T any, M proto.Message](ds dataset.Dataset, convert func(*T) M) (*format.Subject, error) {
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
			NewEncoder: func(w io.Writer) format.Encoder { return delimEncoder{w} },
			NewDecoder: func(r io.Reader) format.Decoder { return delimDecoder{bufio.NewReader(r)} },
		},
		Values: values,
		New:    func() any { var m M; return m.ProtoReflect().New().Interface() },
		Equal:  func(a, b any) bool { return proto.Equal(a.(proto.Message), b.(proto.Message)) },
	}, nil
}

type codec struct{}

func (codec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }

func (codec) Unmarshal(data []byte, v any) error { return proto.Unmarshal(data, v.(proto.Message)) }

type delimEncoder struct {
	w io.Writer
}

func (e delimEncoder) Encode(v any) error {
	_, err := protodelim.MarshalTo(e.w, v.(proto.Message))
	return err
}

type delimDecoder struct {
	r *bufio.Reader
}

func (d delimDecoder) Decode(v any) error { return protodelim.UnmarshalFrom(d.r, v.(proto.Message)) }
