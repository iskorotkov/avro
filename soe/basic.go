package soe

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/iskorotkov/avro/v2"
)

// Codec marshals values to/from bytes, with the Avro binary payload wrapped in
// an SOE frame containing the writer schema fingerprint.
type Codec struct {
	api     avro.API
	schema  avro.Schema
	header  []byte
	writers sync.Pool
}

// NewCodec creates a new Codec for a Schema and the default config.
func NewCodec(schema avro.Schema) (*Codec, error) {
	return NewCodecWithAPI(schema, avro.DefaultConfig)
}

// NewCodecWithAPI creates a new Codec for a Schema and an API.
func NewCodecWithAPI(schema avro.Schema, api avro.API) (*Codec, error) {
	// Precompute SOE header
	header, err := BuildHeader(schema)
	if err != nil {
		return nil, err
	}
	c := &Codec{
		schema: schema,
		api:    api,
		header: header,
	}
	c.writers.New = func() any { return avro.NewWriter(nil, 0, avro.WithWriterConfig(api)) }
	return c, nil
}

// Encode marshals a value to SOE-encoded Avro binary, in a caller-owned slice.
func (c *Codec) Encode(v any) ([]byte, error) {
	return c.encode(v, func(frame []byte) []byte {
		return append(make([]byte, 0, len(frame)), frame...)
	})
}

// AppendEncode appends SOE-encoded Avro binary to dst, reusing its capacity.
func (c *Codec) AppendEncode(dst []byte, v any) ([]byte, error) {
	return c.encode(v, func(frame []byte) []byte {
		return append(dst, frame...)
	})
}

// encode frames v in a pooled writer and hands the frame, valid only during the call, to emit.
func (c *Codec) encode(v any, emit func(frame []byte) []byte) ([]byte, error) {
	w := c.writers.Get().(*avro.Writer)
	defer c.writers.Put(w)

	w.Reset(nil)
	w.Error = nil
	_, _ = w.Write(c.header)
	w.WriteVal(c.schema, v)
	if w.Error != nil {
		return nil, w.Error
	}
	return emit(w.Buffer()), nil
}

// Decode unmarshals a value from SOE-encoded Avro binary, and fails if
// the schema fingerprint doesn't match the held schema.
func (c *Codec) Decode(data []byte, v any) error {
	fingerprint, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	expected := c.getFingerprint()
	if !bytes.Equal(fingerprint, expected) {
		return fmt.Errorf("bad fingerprint %x, expected %x", fingerprint, expected)
	}
	return c.api.Unmarshal(c.schema, data, v)
}

// DecodeUnverified unmarshals a value from SOE-encoded Avro binary without
// validating the schema fingerprint.
func (c *Codec) DecodeUnverified(data []byte, v any) error {
	_, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	return c.api.Unmarshal(c.schema, data, v)
}

func (c *Codec) getFingerprint() []byte {
	// We know the header is well-formed here, so make assumptions.
	return c.header[2:]
}
