// Package heetch adapts github.com/heetch/avro types generated from the dataset schemas.
package heetch

//go:generate go tool avrogo -p superhero -d superhero ../../datasets/superhero/schema.avsc
//go:generate go tool avrogo -p nyctaxi -d nyctaxi ../../datasets/nyctaxi/schema.avsc

import (
	"context"

	"github.com/google/go-cmp/cmp"
	"github.com/heetch/avro"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/format"
)

// Name is the format name.
const Name = "heetch"

// Format binds datasets that have generated types in this package; the library has no streaming or container API.
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
	default:
		return nil, format.ErrUnsupported
	}
}

// bind stores values by value, because Marshal rejects pointers, while New returns pointers to decode into.
func bind[S, T any](ds dataset.Dataset, convert func(*S) T) (*format.Subject, error) {
	values, err := dataset.Convert(ds, convert)
	if err != nil {
		return nil, err
	}

	writerType, err := avro.TypeOf(*new(T))
	if err != nil {
		return nil, err
	}

	return &format.Subject{
		Codec:  codec{decoder: avro.NewSingleDecoder(registry{writerType}, nil)},
		Values: values,
		New:    func() any { return new(T) },
		Equal:  func(a, b any) bool { return cmp.Equal(value[T](a), value[T](b)) },
	}, nil
}

func value[T any](v any) T {
	if p, ok := v.(*T); ok {
		return *p
	}

	return v.(T)
}

// registry maps every message to the single writer type, so SingleDecoder caches its decode program.
type registry struct {
	writerType *avro.Type
}

func (registry) DecodeSchemaID(msg []byte) (int64, []byte) { return 1, msg }

func (r registry) SchemaForID(context.Context, int64) (*avro.Type, error) { return r.writerType, nil }

type codec struct {
	decoder *avro.SingleDecoder
}

func (codec) Marshal(v any) ([]byte, error) {
	data, _, err := avro.Marshal(v)
	return data, err
}

func (c codec) Unmarshal(data []byte, v any) error {
	_, err := c.decoder.Unmarshal(context.Background(), data, v)
	return err
}
