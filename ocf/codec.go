package ocf

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// CodecName represents a compression codec name.
type CodecName string

// Supported compression codecs.
const (
	Null      CodecName = "null"
	Deflate   CodecName = "deflate"
	Snappy    CodecName = "snappy"
	ZStandard CodecName = "zstandard"
)

type codecOptions struct {
	DeflateCompressionLevel int
	ZStandardOptions        zstdOptions
	MaxDecompressedBytes    int
}

type zstdOptions struct {
	EOptions []zstd.EOption
	DOptions []zstd.DOption
	// Encoder and Decoder allow sharing pre-created instances across multiple codecs.
	// When set, EOptions/DOptions are ignored for that component.
	Encoder *zstd.Encoder
	Decoder *zstd.Decoder
}

func resolveCodec(name CodecName, codecOpts codecOptions) (Codec, error) {
	switch name {
	case Null, "":
		return &NullCodec{}, nil

	case Deflate:
		return &DeflateCodec{
			compLvl:         codecOpts.DeflateCompressionLevel,
			maxDecompressed: codecOpts.MaxDecompressedBytes,
		}, nil

	case Snappy:
		return &SnappyCodec{maxDecompressed: codecOpts.MaxDecompressedBytes}, nil

	case ZStandard:
		return newZStandardCodec(codecOpts.ZStandardOptions, codecOpts.MaxDecompressedBytes), nil

	default:
		return nil, fmt.Errorf("unknown codec %s", name)
	}
}

// Codec represents a compression codec.
//
// Built-in codecs reuse internal buffers: a returned slice is only valid until the next call
// to the same method, and a codec must not be used concurrently.
type Codec interface {
	// Decode decodes the given bytes.
	Decode([]byte) ([]byte, error)
	// Encode encodes the given bytes.
	Encode([]byte) []byte
}

// NullCodec is a no op codec.
type NullCodec struct{}

// Decode decodes the given bytes.
func (*NullCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

// Encode encodes the given bytes.
func (*NullCodec) Encode(b []byte) []byte {
	return b
}

// DeflateCodec is a flate compression codec.
type DeflateCodec struct {
	compLvl         int
	maxDecompressed int

	reader io.ReadCloser
	writer *flate.Writer
	in     bytes.Reader
	decBuf bytes.Buffer
	encBuf bytes.Buffer
}

// Decode decodes the given bytes.
func (c *DeflateCodec) Decode(b []byte) ([]byte, error) {
	c.in.Reset(b)
	if c.reader == nil {
		c.reader = flate.NewReader(&c.in)
	} else {
		_ = c.reader.(flate.Resetter).Reset(&c.in, nil)
	}

	var src io.Reader = c.reader
	if c.maxDecompressed > 0 && c.maxDecompressed < math.MaxInt {
		src = io.LimitReader(c.reader, int64(c.maxDecompressed)+1)
	}

	c.decBuf.Reset()
	_, readErr := c.decBuf.ReadFrom(src)
	closeErr := c.reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if c.maxDecompressed > 0 && c.decBuf.Len() > c.maxDecompressed {
		return nil, fmt.Errorf("deflate: decompressed size exceeds %d bytes", c.maxDecompressed)
	}

	return c.decBuf.Bytes(), closeErr
}

// Encode encodes the given bytes.
func (c *DeflateCodec) Encode(b []byte) []byte {
	c.encBuf.Reset()
	if c.writer == nil {
		c.writer, _ = flate.NewWriter(&c.encBuf, c.compLvl)
	} else {
		c.writer.Reset(&c.encBuf)
	}

	_, _ = c.writer.Write(b)
	_ = c.writer.Close()

	return c.encBuf.Bytes()
}

// SnappyCodec is a snappy compression codec.
type SnappyCodec struct {
	maxDecompressed int

	decBuf []byte
	encBuf []byte
}

// Decode decodes the given bytes.
func (c *SnappyCodec) Decode(b []byte) ([]byte, error) {
	l := len(b)
	if l < 5 {
		return nil, errors.New("block does not contain snappy checksum")
	}

	if c.maxDecompressed > 0 {
		dLen, err := snappy.DecodedLen(b[:l-4])
		if err != nil {
			return nil, err
		}
		if dLen > c.maxDecompressed {
			return nil, fmt.Errorf("snappy: claimed decoded size %d exceeds %d bytes", dLen, c.maxDecompressed)
		}
	}

	dst, err := snappy.Decode(c.decBuf[:cap(c.decBuf)], b[:l-4])
	if err != nil {
		return nil, err
	}
	c.decBuf = dst

	crc := binary.BigEndian.Uint32(b[l-4:])
	if crc32.ChecksumIEEE(dst) != crc {
		return nil, errors.New("snappy checksum mismatch")
	}

	return dst, nil
}

// Encode encodes the given bytes.
func (c *SnappyCodec) Encode(b []byte) []byte {
	dst := snappy.Encode(c.encBuf[:cap(c.encBuf)], b)

	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], crc32.ChecksumIEEE(b))
	c.encBuf = dst

	return dst
}

// ZStandardCodec is a zstandard compression codec.
type ZStandardCodec struct {
	decoder         *zstd.Decoder
	encoder         *zstd.Encoder
	sharedDecoder   bool // true if decoder was provided externally and should not be closed
	sharedEncoder   bool // true if encoder was provided externally and should not be closed
	maxDecompressed int

	decBuf []byte
	encBuf []byte
}

func newZStandardCodec(opts zstdOptions, maxDecompressed int) *ZStandardCodec {
	var decoder *zstd.Decoder
	var encoder *zstd.Encoder
	var sharedDecoder, sharedEncoder bool

	switch {
	case opts.Decoder != nil:
		decoder = opts.Decoder
		sharedDecoder = true
	case len(opts.DOptions) == 0:
		decoder, sharedDecoder = defaultZstdDecoders.get(maxDecompressed)
	default:
		decoder = newZstdDecoder(maxDecompressed, opts.DOptions...)
	}

	switch {
	case opts.Encoder != nil:
		encoder = opts.Encoder
		sharedEncoder = true
	case len(opts.EOptions) == 0:
		encoder = defaultZstdEncoder()
		sharedEncoder = true
	default:
		encoder, _ = zstd.NewWriter(nil, opts.EOptions...)
	}

	return &ZStandardCodec{
		decoder:         decoder,
		encoder:         encoder,
		sharedDecoder:   sharedDecoder,
		sharedEncoder:   sharedEncoder,
		maxDecompressed: maxDecompressed,
	}
}

// Decode decodes the given bytes.
func (zstdCodec *ZStandardCodec) Decode(b []byte) ([]byte, error) {
	out, err := zstdCodec.decoder.DecodeAll(b, zstdCodec.decBuf[:0])
	if err != nil {
		return nil, err
	}
	zstdCodec.decBuf = out
	if zstdCodec.maxDecompressed > 0 && len(out) > zstdCodec.maxDecompressed {
		return nil, fmt.Errorf("zstd: decompressed size %d exceeds %d bytes", len(out), zstdCodec.maxDecompressed)
	}
	return out, nil
}

// Encode encodes the given bytes.
func (zstdCodec *ZStandardCodec) Encode(b []byte) []byte {
	zstdCodec.encBuf = zstdCodec.encoder.EncodeAll(b, zstdCodec.encBuf[:0])
	return zstdCodec.encBuf
}

// Close closes the zstandard encoder and decoder, releasing resources.
// Shared instances (provided via WithZStandardEncoder/WithZStandardDecoder) are not closed.
func (zstdCodec *ZStandardCodec) Close() error {
	if zstdCodec.decoder != nil && !zstdCodec.sharedDecoder {
		zstdCodec.decoder.Close()
	}
	if zstdCodec.encoder != nil && !zstdCodec.sharedEncoder {
		return zstdCodec.encoder.Close()
	}
	return nil
}
