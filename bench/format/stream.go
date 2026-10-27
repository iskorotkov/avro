package format

import (
	"bytes"
	"fmt"
	"io"
)

// Encoder writes values to a stream.
type Encoder interface {
	Encode(v any) error
}

// Decoder reads values from a stream.
type Decoder interface {
	Decode(v any) error
}

// EncodeCloser writes values to a stream that must be closed to flush it.
type EncodeCloser interface {
	Encoder
	io.Closer
}

// StreamBatch is a BatchCodec built on a format's streaming encoder and decoder.
type StreamBatch struct {
	NewEncoder func(w io.Writer) Encoder
	NewDecoder func(r io.Reader) Decoder

	buf bytes.Buffer
}

// MarshalBatch implements BatchCodec.
func (s *StreamBatch) MarshalBatch(vs []any) ([]byte, error) {
	s.buf.Reset()
	if err := encodeAll(s.NewEncoder(&s.buf), vs); err != nil {
		return nil, err
	}

	return s.buf.Bytes(), nil
}

// UnmarshalBatch implements BatchCodec.
func (s *StreamBatch) UnmarshalBatch(data []byte, vs []any) error {
	return decodeAll(s.NewDecoder(bytes.NewReader(data)), vs)
}

// StreamContainer is a ContainerCodec built on a format's container file encoder and decoder.
type StreamContainer struct {
	Codecs     []string
	NewEncoder func(w io.Writer, compression string) (EncodeCloser, error)
	NewDecoder func(r io.Reader) (Decoder, error)
}

// Compressions implements ContainerCodec.
func (s *StreamContainer) Compressions() []string { return s.Codecs }

// WriteContainer implements ContainerCodec.
func (s *StreamContainer) WriteContainer(w io.Writer, compression string, vs []any) error {
	enc, err := s.NewEncoder(w, compression)
	if err != nil {
		return err
	}
	if err := encodeAll(enc, vs); err != nil {
		_ = enc.Close()
		return err
	}

	return enc.Close()
}

// ReadContainer implements ContainerCodec.
func (s *StreamContainer) ReadContainer(r io.Reader, vs []any) error {
	dec, err := s.NewDecoder(r)
	if err != nil {
		return err
	}

	return decodeAll(dec, vs)
}

func encodeAll(enc Encoder, vs []any) error {
	for i, v := range vs {
		if err := enc.Encode(v); err != nil {
			return fmt.Errorf("encode value %d: %w", i, err)
		}
	}

	return nil
}

func decodeAll(dec Decoder, vs []any) error {
	for i, v := range vs {
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("decode value %d: %w", i, err)
		}
	}

	return nil
}
