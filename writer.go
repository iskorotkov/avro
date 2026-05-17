package avro

import (
	"encoding/binary"
	"io"
	"math"
)

// WriterFunc is a function used to customize the Writer.
type WriterFunc func(w *Writer)

// WithWriterConfig specifies the configuration to use with a writer.
func WithWriterConfig(cfg API) WriterFunc {
	return func(w *Writer) {
		w.cfg = cfg.(*frozenConfig)
	}
}

// Writer is an Avro specific io.Writer.
type Writer struct {
	cfg   *frozenConfig
	out   io.Writer
	buf   []byte
	Error error
}

// NewWriter creates a new Writer.
func NewWriter(out io.Writer, bufSize int, opts ...WriterFunc) *Writer {
	writer := &Writer{
		cfg: DefaultConfig.(*frozenConfig),
		out: out,
	}
	for _, opt := range opts {
		opt(writer)
	}
	writer.buf = make([]byte, 0, max(0, writer.cfg.getWriteBufSize()))
	return writer
}

// Reset resets the Writer with a new io.Writer attached.
func (w *Writer) Reset(out io.Writer) {
	w.out = out
	w.buf = w.buf[:0]
}

// Buffered returns the number of buffered bytes.
func (w *Writer) Buffered() int {
	return len(w.buf)
}

// Buffer gets the Writer buffer.
func (w *Writer) Buffer() []byte {
	return w.buf
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.out == nil {
		return nil
	}
	if w.Error != nil {
		return w.Error
	}

	n, err := w.out.Write(w.buf)
	if n < len(w.buf) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if w.Error == nil {
			w.Error = err
		}
		return err
	}

	w.buf = w.buf[:0]

	return nil
}

func (w *Writer) writeByte(b byte) {
	w.buf = append(w.buf, b)
}

// Write writes raw bytes to the Writer.
func (w *Writer) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return len(b), nil
}

// WriteBool writes a Bool to the Writer.
func (w *Writer) WriteBool(b bool) {
	if b {
		w.writeByte(0x01)
		return
	}
	w.writeByte(0x00)
}

// WriteInt writes an Int to the Writer.
func (w *Writer) WriteInt(i int32) {
	e := uint64((uint32(i) << 1) ^ uint32(i>>31))
	w.encodeInt(e)
}

// WriteLong writes a Long to the Writer.
func (w *Writer) WriteLong(i int64) {
	e := (uint64(i) << 1) ^ uint64(i>>63)
	w.encodeInt(e)
}

func (w *Writer) encodeInt(i uint64) {
	if i < 0x80 {
		w.buf = append(w.buf, byte(i))
		return
	}
	if i < 0x4000 {
		w.buf = append(w.buf, byte(i)|0x80, byte(i>>7))
		return
	}

	w.buf = binary.AppendUvarint(w.buf, i)
}

// WriteFloat writes a Float to the Writer.
func (w *Writer) WriteFloat(f float32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(f))

	w.buf = append(w.buf, b...)
}

// WriteDouble writes a Double to the Writer.
func (w *Writer) WriteDouble(f float64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(f))

	w.buf = append(w.buf, b...)
}

// WriteBytes writes Bytes to the Writer.
func (w *Writer) WriteBytes(b []byte) {
	w.WriteLong(int64(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteString reads a String to the Writer.
func (w *Writer) WriteString(s string) {
	w.WriteLong(int64(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteBlockHeader writes a Block Header to the Writer.
func (w *Writer) WriteBlockHeader(l, s int64) {
	if s > 0 && !w.cfg.config.DisableBlockSizeHeader {
		w.WriteLong(-l)
		w.WriteLong(s)
		return
	}
	w.WriteLong(l)
}

const writeBlockHeaderMaxBytes = 20

// WriteBlockCB writes a block using the callback.
func (w *Writer) WriteBlockCB(callback func(w *Writer) int64) int64 {
	headerStart := len(w.buf)
	capturedAt := headerStart + writeBlockHeaderMaxBytes
	if capturedAt > cap(w.buf) {
		var pad [writeBlockHeaderMaxBytes]byte
		w.buf = append(w.buf, pad[:]...)
	} else {
		w.buf = w.buf[:capturedAt]
	}

	length := callback(w)
	blockEnd := len(w.buf)
	size := int64(blockEnd - capturedAt)

	w.buf = w.buf[:headerStart]
	w.WriteBlockHeader(length, size)
	hdrEnd := len(w.buf)

	if hdrEnd < capturedAt {
		copy(w.buf[hdrEnd:hdrEnd+int(size)], w.buf[capturedAt:blockEnd])
	}
	w.buf = w.buf[:hdrEnd+int(size)]

	return length
}
