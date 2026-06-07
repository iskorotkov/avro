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
}

// Decode decodes the given bytes.
func (c *DeflateCodec) Decode(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewBuffer(b))

	var src io.Reader = r
	if c.maxDecompressed > 0 && c.maxDecompressed < math.MaxInt {
		src = io.LimitReader(r, int64(c.maxDecompressed)+1)
	}

	data, readErr := io.ReadAll(src)
	closeErr := r.Close()
	if readErr != nil {
		return nil, readErr
	}
	if c.maxDecompressed > 0 && len(data) > c.maxDecompressed {
		return nil, fmt.Errorf("deflate: decompressed size exceeds %d bytes", c.maxDecompressed)
	}

	return data, closeErr
}

// Encode encodes the given bytes.
func (c *DeflateCodec) Encode(b []byte) []byte {
	data := bytes.NewBuffer(make([]byte, 0, len(b)))

	w, _ := flate.NewWriter(data, c.compLvl)
	_, _ = w.Write(b)
	_ = w.Close()

	return data.Bytes()
}

// SnappyCodec is a snappy compression codec.
type SnappyCodec struct {
	maxDecompressed int
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

	dst, err := snappy.Decode(nil, b[:l-4])
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(b[l-4:])
	if crc32.ChecksumIEEE(dst) != crc {
		return nil, errors.New("snappy checksum mismatch")
	}

	return dst, nil
}

// Encode encodes the given bytes.
func (*SnappyCodec) Encode(b []byte) []byte {
	dst := snappy.Encode(nil, b)

	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], crc32.ChecksumIEEE(b))

	return dst
}

// ZStandardCodec is a zstandard compression codec.
type ZStandardCodec struct {
	decoder         *zstd.Decoder
	encoder         *zstd.Encoder
	sharedDecoder   bool // true if decoder was provided externally and should not be closed
	sharedEncoder   bool // true if encoder was provided externally and should not be closed
	maxDecompressed int
}

func newZStandardCodec(opts zstdOptions, maxDecompressed int) *ZStandardCodec {
	var decoder *zstd.Decoder
	var encoder *zstd.Encoder
	var sharedDecoder, sharedEncoder bool

	if opts.Decoder != nil {
		decoder = opts.Decoder
		sharedDecoder = true
	} else {
		dOpts := opts.DOptions
		if maxDecompressed > 0 {
			dOpts = append([]zstd.DOption{zstd.WithDecoderMaxMemory(uint64(maxDecompressed))}, dOpts...)
		}
		decoder, _ = zstd.NewReader(nil, dOpts...)
	}

	if opts.Encoder != nil {
		encoder = opts.Encoder
		sharedEncoder = true
	} else {
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
	out, err := zstdCodec.decoder.DecodeAll(b, nil)
	if err != nil {
		return nil, err
	}
	if zstdCodec.maxDecompressed > 0 && len(out) > zstdCodec.maxDecompressed {
		return nil, fmt.Errorf("zstd: decompressed size %d exceeds %d bytes", len(out), zstdCodec.maxDecompressed)
	}
	return out, nil
}

// Encode encodes the given bytes.
func (zstdCodec *ZStandardCodec) Encode(b []byte) []byte {
	return zstdCodec.encoder.EncodeAll(b, nil)
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
