// Package goavro adapts github.com/linkedin/goavro/v2, which works on generic maps, to the benchmark suite.
package goavro

import (
	"errors"
	"io"

	"github.com/google/go-cmp/cmp"
	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/datasets/nyctaxi"
	"github.com/iskorotkov/avro/bench/datasets/superhero"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/linkedin/goavro/v2"
)

// Name is the format name.
const Name = "goavro"

// Same block length as the other adapters.
const blockLength = 100

// Record is the generic representation of a record; unions hold nil or a single-entry map keyed by type name.
type Record = map[string]any

// Format binds the datasets that have a converter to generic maps.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	var values []any
	var err error
	switch ds.Name() {
	case superhero.Name:
		values, err = dataset.Convert(ds, fromSuperhero)
	case nyctaxi.Name:
		values, err = dataset.Convert(ds, fromTrip)
	default:
		return nil, format.ErrUnsupported
	}
	if err != nil {
		return nil, err
	}

	c, err := goavro.NewCodec(ds.Schema())
	if err != nil {
		return nil, err
	}

	return &format.Subject{
		Codec:     &codec{codec: c},
		Batch:     &batch{codec: c},
		Container: &container{schema: ds.Schema()},
		Values:    values,
		New:       func() any { return new(Record) },
		Equal:     equal,
	}, nil
}

// equal compares records held either directly or behind the pointers returned by New.
func equal(a, b any) bool { return cmp.Equal(record(a), record(b)) }

func record(v any) Record {
	if p, ok := v.(*Record); ok {
		return *p
	}

	return v.(Record)
}

type codec struct {
	codec *goavro.Codec
	buf   []byte
}

func (c *codec) Marshal(v any) ([]byte, error) {
	var err error
	c.buf, err = c.codec.BinaryFromNative(c.buf[:0], v)

	return c.buf, err
}

func (c *codec) Unmarshal(data []byte, v any) error {
	native, _, err := c.codec.NativeFromBinary(data)
	if err != nil {
		return err
	}
	*v.(*Record) = native.(Record)

	return nil
}

// batch concatenates values, as the library has no streaming encoder.
type batch struct {
	codec *goavro.Codec
	buf   []byte
}

func (b *batch) MarshalBatch(vs []any) ([]byte, error) {
	b.buf = b.buf[:0]
	for _, v := range vs {
		var err error
		if b.buf, err = b.codec.BinaryFromNative(b.buf, v); err != nil {
			return nil, err
		}
	}

	return b.buf, nil
}

func (b *batch) UnmarshalBatch(data []byte, vs []any) error {
	for _, v := range vs {
		native, rest, err := b.codec.NativeFromBinary(data)
		if err != nil {
			return err
		}
		*v.(*Record), data = native.(Record), rest
	}

	return nil
}

// container uses the library's OCF writer and reader, which support no zstd.
type container struct {
	schema string
}

func (*container) Compressions() []string { return []string{"null", "deflate", "snappy"} }

func (c *container) WriteContainer(w io.Writer, compression string, vs []any) error {
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{W: w, Schema: c.schema, CompressionName: compression})
	if err != nil {
		return err
	}

	for len(vs) > 0 {
		n := min(blockLength, len(vs))
		if err := ocfw.Append(vs[:n]); err != nil {
			return err
		}
		vs = vs[n:]
	}

	return nil
}

func (*container) ReadContainer(r io.Reader, vs []any) error {
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return err
	}

	for _, v := range vs {
		if !ocfr.Scan() {
			return errors.Join(ocfr.Err(), io.ErrUnexpectedEOF)
		}
		native, err := ocfr.Read()
		if err != nil {
			return err
		}
		*v.(*Record) = native.(Record)
	}

	return nil
}
