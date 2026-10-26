package avro

import (
	"errors"
	"fmt"
	"io"
)

// Decoder reads and decodes Avro values from an input stream.
type Decoder struct {
	s Schema
	r *Reader
}

// NewDecoder returns a new decoder that reads from reader r using schema s.
func NewDecoder(s string, r io.Reader) (*Decoder, error) {
	sch, err := Parse(s)
	if err != nil {
		return nil, err
	}

	return NewDecoderForSchema(sch, r), nil
}

// NewDecoderForSchema returns a new decoder that reads from r using schema.
func NewDecoderForSchema(schema Schema, reader io.Reader) *Decoder {
	return DefaultConfig.NewDecoder(schema, reader)
}

// Decode reads the next Avro encoded value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(v any) error {
	if d.r.head == d.r.tail && d.r.reader != nil {
		if !d.r.loadMore() {
			return io.EOF
		}
	}

	d.r.ReadVal(d.s, v)
	return valueError(d.r.Error)
}

// valueError reports input that ends inside a value as io.ErrUnexpectedEOF.
func valueError(err error) error {
	switch {
	case err == nil, !errors.Is(err, io.EOF):
		return err
	//nolint:errorlint // A bare EOF is replaced without extra context.
	case err == io.EOF:
		return io.ErrUnexpectedEOF
	default:
		return fmt.Errorf("%w: %s", io.ErrUnexpectedEOF, err.Error())
	}
}

// Unmarshal parses the Avro encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, Unmarshal returns an error.
func Unmarshal(schema Schema, data []byte, v any) error {
	return DefaultConfig.Unmarshal(schema, data, v)
}
