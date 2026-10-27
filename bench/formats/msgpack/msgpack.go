// Package msgpack adapts github.com/vmihailenco/msgpack/v5 (reflection) to the benchmark suite.
package msgpack

import (
	"bytes"
	"io"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/vmihailenco/msgpack/v5"
)

// Name is the format name.
const Name = "msgpack"

// Format binds any dataset through the json struct tags of its canonical records.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	return &format.Subject{
		Codec: newCodec(),
		Batch: &format.StreamBatch{
			NewEncoder: func(w io.Writer) format.Encoder { return newEncoder(w) },
			NewDecoder: func(r io.Reader) format.Decoder { return newDecoder(r) },
		},
		Values: ds.Records(),
		New:    ds.New,
	}, nil
}

// codec allocates a fresh output buffer per call like the other adapters; the decoder is reused.
type codec struct {
	rd  bytes.Reader
	dec *msgpack.Decoder
}

func newCodec() *codec {
	c := new(codec)
	c.dec = newDecoder(&c.rd)

	return c
}

func (*codec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := newEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *codec) Unmarshal(data []byte, v any) error {
	c.rd.Reset(data)
	c.dec.Reset(&c.rd)
	c.dec.SetCustomStructTag("json")

	return c.dec.Decode(v)
}

func newEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.SetCustomStructTag("json")

	return enc
}

func newDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.SetCustomStructTag("json")

	return dec
}
