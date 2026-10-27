// Package vtprotobuf adapts planetscale/vtprotobuf generated MarshalVT/UnmarshalVT methods to the benchmark suite.
package vtprotobuf

import (
	"bufio"
	"encoding/binary"
	"io"
	"slices"

	"github.com/iskorotkov/avro/bench/dataset"
	"github.com/iskorotkov/avro/bench/format"
	"github.com/iskorotkov/avro/bench/formats/protobuf"
	"google.golang.org/protobuf/encoding/protowire"
)

// Name is the format name.
const Name = "vtprotobuf"

// message is a generated message with vtprotobuf methods.
type message interface {
	SizeVT() int
	MarshalVT() ([]byte, error)
	MarshalToSizedBufferVT(data []byte) (int, error)
	UnmarshalVT(data []byte) error
}

// Format binds the datasets of the protobuf format, swapping its codecs for the vtprotobuf methods.
type Format struct{}

// Name implements format.Format.
func (Format) Name() string { return Name }

// Bind implements format.Format.
func (Format) Bind(ds dataset.Dataset) (*format.Subject, error) {
	s, err := protobuf.Format{}.Bind(ds)
	if err != nil {
		return nil, err
	}

	vt := *s
	vt.Codec = codec{}
	vt.Batch = &format.StreamBatch{
		NewEncoder: func(w io.Writer) format.Encoder { return &delimEncoder{w: w} },
		NewDecoder: func(r io.Reader) format.Decoder { return &delimDecoder{r: bufio.NewReader(r)} },
	}

	return &vt, nil
}

type codec struct{}

func (codec) Marshal(v any) ([]byte, error) { return v.(message).MarshalVT() }

func (codec) Unmarshal(data []byte, v any) error { return v.(message).UnmarshalVT(data) }

// delimEncoder writes varint size-delimited messages, the same framing as protodelim.
type delimEncoder struct {
	w   io.Writer
	buf []byte
}

func (e *delimEncoder) Encode(v any) error {
	m := v.(message)
	size := m.SizeVT()
	e.buf = protowire.AppendVarint(e.buf[:0], uint64(size))
	head := len(e.buf)
	e.buf = slices.Grow(e.buf, size)[:head+size]
	if _, err := m.MarshalToSizedBufferVT(e.buf[head:]); err != nil {
		return err
	}
	_, err := e.w.Write(e.buf)

	return err
}

type delimDecoder struct {
	r   *bufio.Reader
	buf []byte
}

func (d *delimDecoder) Decode(v any) error {
	size, err := binary.ReadUvarint(d.r)
	if err != nil {
		return err
	}
	d.buf = slices.Grow(d.buf[:0], int(size))[:size]
	if _, err := io.ReadFull(d.r, d.buf); err != nil {
		return err
	}

	return v.(message).UnmarshalVT(d.buf)
}
