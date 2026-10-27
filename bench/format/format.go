// Package format defines the adapter interface every compared serialisation format implements.
package format

import (
	"errors"
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
)

// ErrUnsupported is returned by Format.Bind when a format has no binding for a dataset.
var ErrUnsupported = errors.New("format does not support dataset")

// Format is a serialisation library under comparison.
type Format interface {
	// Name is the stable identifier used in benchmark names.
	Name() string
	// Bind prepares the format for a dataset, converting records to native values.
	Bind(ds dataset.Dataset) (*Subject, error)
}

// Codec encodes and decodes single native values and is safe for concurrent use.
type Codec interface {
	// Marshal encodes v into a new slice owned by the caller.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes data into v, a value returned by Subject.New.
	Unmarshal(data []byte, v any) error
}

// BatchCodec encodes and decodes a sequence of native values as one stream.
type BatchCodec interface {
	// MarshalBatch encodes vs; the result is valid until the next call.
	MarshalBatch(vs []any) ([]byte, error)
	// UnmarshalBatch decodes len(vs) values from data into vs.
	UnmarshalBatch(data []byte, vs []any) error
}

// ContainerCodec writes and reads self-describing container files.
type ContainerCodec interface {
	// Compressions lists the supported block compression names.
	Compressions() []string
	// WriteContainer writes vs as one container file compressed with compression.
	WriteContainer(w io.Writer, compression string, vs []any) error
	// ReadContainer reads len(vs) values from a container file into vs.
	ReadContainer(r io.Reader, vs []any) error
}

// Subject is a format bound to a dataset.
type Subject struct {
	// Codec encodes single values.
	Codec Codec
	// Batch encodes value streams; nil when unsupported.
	Batch BatchCodec
	// Container writes container files; nil when unsupported.
	Container ContainerCodec
	// Values are the dataset records in the format's native representation.
	Values []any
	// New returns a new zero native value.
	New func() any
	// Equal reports whether two native values are equal; nil means cmp.Equal.
	Equal func(a, b any) bool
}

// NewValues returns n new zero native values.
func (s *Subject) NewValues(n int) []any {
	vs := make([]any, n)
	for i := range vs {
		vs[i] = s.New()
	}

	return vs
}
