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

// Both reflection adapters set these explicitly to the same values so they run with equivalent options.
const (
	blockLength      = 100
	maxByteSliceSize = 1 << 20
)

var api = avro.Config{TagKey: "avro", BlockLength: blockLength, MaxByteSliceSize: maxByteSliceSize}.Freeze()

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

	return Subject(schema, ds.Records(), ds.New), nil
}

// Subject binds values of any Go type the library can map onto schema.
func Subject(schema avro.Schema, values []any, newValue func() any) *format.Subject {
	return &format.Subject{
		Codec: codec{schema: schema},
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return api.NewEncoder(schema, w) },
			NewDecoder: func(r io.Reader) format.Decoder { return api.NewDecoder(schema, r) },
		},
		Container: &format.StreamContainer{
			Codecs: []string{"null", "deflate", "snappy", "zstd"},
			NewEncoder: func(w io.Writer, compression string) (format.EncodeCloser, error) {
				return ocf.NewEncoderWithSchema(schema, w,
					ocf.WithCodec(compressions[compression]),
					ocf.WithBlockLength(blockLength),
					ocf.WithEncodingConfig(api),
				)
			},
			NewDecoder: func(r io.Reader) (format.Decoder, error) {
				dec, err := ocf.NewDecoder(r, ocf.WithDecoderConfig(api))
				return ocfDecoder{dec}, err
			},
		},
		Values: values,
		New:    newValue,
	}
}

type codec struct {
	schema avro.Schema
}

func (c codec) Marshal(v any) ([]byte, error) { return api.Marshal(c.schema, v) }

func (c codec) Unmarshal(data []byte, v any) error { return api.Unmarshal(c.schema, data, v) }

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
