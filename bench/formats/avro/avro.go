// Package avro adapts this library's reflection API to the benchmark suite.
package avro

import (
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"
)

// Name is the format name; the report normalises results to it.
const Name = "avro"

var compressions = map[string]ocf.CodecName{
	"null":    ocf.Null,
	"deflate": ocf.Deflate,
	"snappy":  ocf.Snappy,
	"zstd":    ocf.ZStandard,
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
		Codec: codec{schema: schema},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return avro.NewEncoderForSchema(schema, w) },
			NewDecoder: func(r io.Reader) format.Decoder { return avro.NewDecoderForSchema(schema, r) },
		},
		Container: &format.StreamContainer{
			Codecs: []string{"null", "deflate", "snappy", "zstd"},
			NewEncoder: func(w io.Writer, compression string) (format.EncodeCloser, error) {
				return ocf.NewEncoderWithSchema(schema, w, ocf.WithCodec(compressions[compression]))
			},
			NewDecoder: func(r io.Reader) (format.Decoder, error) {
				dec, err := ocf.NewDecoder(r)
				return ocfDecoder{dec}, err
			},
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

type codec struct {
	schema avro.Schema
}

func (c codec) Marshal(v any) ([]byte, error) { return avro.Marshal(c.schema, v) }

func (c codec) Unmarshal(data []byte, v any) error { return avro.Unmarshal(c.schema, data, v) }

type ocfDecoder struct {
	dec *ocf.Decoder
}

func (d ocfDecoder) Decode(v any) error {
	if !d.dec.HasNext() {
		if err := d.dec.Error(); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}

	return d.dec.Decode(v)
}
