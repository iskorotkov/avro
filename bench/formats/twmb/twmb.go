// Package twmb adapts github.com/twmb/avro to the benchmark suite.
package twmb

import (
	"compress/flate"
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// Name is the format name.
const Name = "twmb"

// Same block length as the other reflection adapters.
const blockLength = 100

var codecs = map[string]func() ocf.Codec{
	"null":    func() ocf.Codec { return nil },
	"deflate": func() ocf.Codec { return ocf.DeflateCodec(flate.DefaultCompression) },
	"snappy":  ocf.SnappyCodec,
	"zstd":    func() ocf.Codec { return ocf.MustZstdCodec(nil, nil) },
}

// Format binds any dataset through reflection on its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	schema, err := avro.Parse(ds.Schema())
	if err != nil {
		return nil, err
	}

	return &format.Subject{
		Codec: &codec{schema: schema},
		Batch: &batch{schema: schema},
		Container: &format.StreamContainer{
			Codecs: []string{"null", "deflate", "snappy", "zstd"},
			NewEncoder: func(w io.Writer, compression string) (format.EncodeCloser, error) {
				return ocf.NewWriter(w, schema, ocf.WithCodec(codecs[compression]()), ocf.WithBlockCount(blockLength))
			},
			NewDecoder: func(r io.Reader) (format.Decoder, error) { return ocf.NewReader(r) },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct {
	schema *avro.Schema
	buf    []byte
}

func (c *codec) Marshal(v any) ([]byte, error) {
	var err error
	c.buf, err = c.schema.AppendEncode(c.buf[:0], v)

	return c.buf, err
}

func (c *codec) Unmarshal(data []byte, v any) error {
	_, err := c.schema.Decode(data, v)
	return err
}

// batch concatenates values, as the library has no streaming encoder.
type batch struct {
	schema *avro.Schema
	buf    []byte
}

func (b *batch) MarshalBatch(vs []any) ([]byte, error) {
	b.buf = b.buf[:0]
	for _, v := range vs {
		var err error
		if b.buf, err = b.schema.AppendEncode(b.buf, v); err != nil {
			return nil, err
		}
	}

	return b.buf, nil
}

func (b *batch) UnmarshalBatch(data []byte, vs []any) error {
	for _, v := range vs {
		var err error
		if data, err = b.schema.Decode(data, v); err != nil {
			return err
		}
	}

	return nil
}
