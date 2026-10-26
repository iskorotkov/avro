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
	writers *sync.Pool // nil when api cannot back an avro.Writer
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
	return &Codec{
		schema:  schema,
		api:     api,
		header:  header,
		writers: newWriterPool(api),
	}, nil
}

// newWriterPool returns nil for APIs that avro.WithWriterConfig rejects, such as custom wrappers.
func newWriterPool(api avro.API) (pool *sync.Pool) {
	defer func() {
		if recover() != nil {
			pool = nil
		}
	}()

	_ = avro.NewWriter(nil, 0, avro.WithWriterConfig(api))
	return &sync.Pool{New: func() any { return avro.NewWriter(nil, 0, avro.WithWriterConfig(api)) }}
}

// Encode marshals a value to SOE-encoded Avro binary, in a caller-owned slice.
func (c *Codec) Encode(v any) ([]byte, error) {
	return c.encode(v, func(head, body []byte) []byte {
		buf := make([]byte, 0, len(head)+len(body))
		buf = append(buf, head...)
		return append(buf, body...)
	})
}

// AppendEncode appends SOE-encoded Avro binary to dst, reusing its capacity.
func (c *Codec) AppendEncode(dst []byte, v any) ([]byte, error) {
	return c.encode(v, func(head, body []byte) []byte {
		dst = append(dst, head...)
		return append(dst, body...)
	})
}

// encode hands the SOE frame of v, split in two parts valid only during the call, to emit.
func (c *Codec) encode(v any, emit func(head, body []byte) []byte) ([]byte, error) {
	if c.writers == nil {
		data, err := c.api.Marshal(c.schema, v)
		if err != nil {
			return nil, err
		}
		return emit(c.header, data), nil
	}

	w := c.writers.Get().(*avro.Writer)
	defer c.writers.Put(w)

	w.Reset(nil)
	w.Error = nil
	_, _ = w.Write(c.header)
	w.WriteVal(c.schema, v)
	if w.Error != nil {
		return nil, w.Error
	}
	return emit(w.Buffer(), nil), nil
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
