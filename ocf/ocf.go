// Package ocf implements encoding and decoding of Avro Object Container Files as defined by the Avro specification.
//
// See the Avro specification for an understanding of Avro: http://avro.apache.org/docs/current/
package ocf

import (
	"compress/flate"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"slices"

	"github.com/iskorotkov/avro/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/modern-go/reflect2"
)

const (
	schemaKey = "avro.schema"
	codecKey  = "avro.codec"
)

var (
	magicBytes = [4]byte{'O', 'b', 'j', 1}

	// HeaderSchema is the Avro schema of a container file header.
	HeaderSchema = avro.MustParse(`{
	"type": "record",
	"name": "org.apache.avro.file.Header",
	"fields": [
		{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
		{"name": "meta", "type": {"type": "map", "values": "bytes"}},
		{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
	]
}`)

	// DefaultSchemaMarshaler calls the schema's String() method, to produce
	// a "canonical" schema.
	DefaultSchemaMarshaler = defaultMarshalSchema
	// FullSchemaMarshaler calls the schema's MarshalJSON() method, to produce
	// a schema with all details preserved. The "canonical" schema returned by
	// the default marshaler does not preserve a type's extra properties.
	FullSchemaMarshaler = fullMarshalSchema
)

// Header represents an Avro container file header.
type Header struct {
	Magic [4]byte           `avro:"magic"`
	Meta  map[string][]byte `avro:"meta"`
	Sync  [16]byte          `avro:"sync"`
}

type decoderConfig struct {
	DecoderConfig             avro.API
	SchemaCache               *avro.SchemaCache
	CodecOptions              codecOptions
	MaxBlockBytes             int
	MaxDecompressedBlockBytes int
	MaxZeroWidthBlockCount    int64
}

// DecoderFunc represents a configuration function for Decoder.
type DecoderFunc func(cfg *decoderConfig)

// WithDecoderConfig sets the value decoder config on the OCF decoder.
func WithDecoderConfig(wCfg avro.API) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.DecoderConfig = wCfg
	}
}

// WithDecoderSchemaCache sets the schema cache for the decoder.
// If not specified, defaults to avro.DefaultSchemaCache.
func WithDecoderSchemaCache(cache *avro.SchemaCache) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.SchemaCache = cache
	}
}

// WithZStandardDecoderOptions sets the options for the ZStandard decoder.
func WithZStandardDecoderOptions(opts ...zstd.DOption) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.CodecOptions.ZStandardOptions.DOptions = append(cfg.CodecOptions.ZStandardOptions.DOptions, opts...)
	}
}

// WithZStandardDecoder sets a pre-created ZStandard decoder to be reused.
// This allows sharing a single decoder across multiple OCF decoders for efficiency.
// The caller is responsible for closing the decoder after all OCF decoders are done.
func WithZStandardDecoder(dec *zstd.Decoder) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.CodecOptions.ZStandardOptions.Decoder = dec
	}
}

// WithMaxBlockBytes caps the declared compressed size of a single OCF data block; a header claiming more errors out before any allocation. Values <= 0 disable the check (the default).
//
// It does not bound decompression amplification — a small block can still expand much larger; combine it with WithMaxDecompressedBlockBytes.
func WithMaxBlockBytes(n int) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.MaxBlockBytes = n
	}
}

// WithMaxDecompressedBlockBytes caps the size of a single OCF data block after the codec decompresses it; built-in codecs (deflate, snappy, zstandard) reject inputs that would expand past it. Values <= 0 disable the check (the default).
//
// Oversized blocks fail with an error wrapping ErrDecompressedBlockTooLarge. The zstandard decoder accepts frame windows up to 8 MiB, so it may allocate on the order of 2 × max(n, 8 MiB) before rejecting a block.
//
// It does not apply to user-supplied codecs registered outside this package; with a zstandard decoder shared via WithZStandardDecoder it still rejects oversized output but cannot prevent the allocation, so set zstd.WithDecoderMaxMemory on the shared decoder to reject earlier.
func WithMaxDecompressedBlockBytes(n int) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.MaxDecompressedBlockBytes = n
	}
}

// WithMaxZeroWidthBlockCount caps the record count of a block whose schema encodes to zero bytes, such as "null" or an empty record. Values <= 0 restore the default of DefaultMaxZeroWidthBlockCount.
func WithMaxZeroWidthBlockCount(n int64) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.MaxZeroWidthBlockCount = n
	}
}

// DefaultMaxZeroWidthBlockCount is the default record-count cap for blocks of a zero-width schema.
const DefaultMaxZeroWidthBlockCount = 1 << 24

// Decoder reads Avro values from a container file and rejects blocks whose record count cannot fit their bytes.
type Decoder struct {
	reader   *avro.Reader
	block    *avro.Reader
	blockBuf []byte
	api      avro.API
	decoders typeCache[avro.ValDecoder]
	meta     map[string][]byte
	sync     [16]byte
	schema   avro.Schema

	codec Codec

	count             int64
	maxBlockBytes     int
	maxZeroWidthCount int64
	minValueBytes     int
}

// NewDecoder returns a new decoder that reads from r; WithDecoderConfig limits bound the header size.
func NewDecoder(r io.Reader, opts ...DecoderFunc) (*Decoder, error) {
	cfg := decoderConfig{
		DecoderConfig: avro.DefaultConfig,
		SchemaCache:   avro.DefaultSchemaCache,
		CodecOptions: codecOptions{
			DeflateCompressionLevel: flate.DefaultCompression,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.MaxZeroWidthBlockCount <= 0 {
		cfg.MaxZeroWidthBlockCount = DefaultMaxZeroWidthBlockCount
	}

	cfg.CodecOptions.MaxDecompressedBytes = cfg.MaxDecompressedBlockBytes

	reader := avro.NewReader(r, 0, avro.WithReaderConfig(cfg.DecoderConfig))

	h, err := readHeader(reader, cfg.SchemaCache, cfg.CodecOptions)
	if err != nil {
		return nil, fmt.Errorf("decoder: %w", err)
	}

	return &Decoder{
		reader:            reader,
		block:             avro.NewReader(nil, 0, avro.WithReaderConfig(cfg.DecoderConfig)),
		api:               cfg.DecoderConfig,
		meta:              h.Meta,
		sync:              h.Sync,
		codec:             h.Codec,
		schema:            h.Schema,
		maxBlockBytes:     cfg.MaxBlockBytes,
		maxZeroWidthCount: cfg.MaxZeroWidthBlockCount,
		minValueBytes:     minWireSize(h.Schema),
	}, nil
}

// Metadata returns the header metadata.
func (d *Decoder) Metadata() map[string][]byte {
	return d.meta
}

// Schema returns the schema that was parsed from the file's metadata
// and that is used to interpret the file's contents.
func (d *Decoder) Schema() avro.Schema {
	return d.schema
}

// HasNext determines if there is another value to read.
func (d *Decoder) HasNext() bool {
	if d.count <= 0 && d.reader.Error == nil && d.blockConsumed() {
		// Empty blocks are skipped; readBlock stops the loop with an error at the end of the input.
		d.count = d.readBlock()
		for d.count == 0 && d.reader.Error == nil {
			d.count = d.readBlock()
		}
	}

	if d.reader.Error != nil {
		return false
	}

	return d.count > 0
}

// Decode reads the next Avro encoded value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(v any) error {
	if d.count <= 0 {
		return errors.New("decoder: no data found, call HasNext first")
	}

	dec, err := d.decoders.get(v, d.decoderOf)
	if err != nil {
		return err
	}
	ptr := reflect2.PtrOf(v)
	if ptr == nil {
		return errors.New("decoder: can not read into nil pointer")
	}

	d.count--
	dec.Decode(ptr, d.block)

	if err = d.block.Error; err != nil {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("decoder: block ended mid-value: %w", io.ErrUnexpectedEOF)
		}
		d.reader.Error = err
		return err
	}
	return nil
}

func (d *Decoder) decoderOf(typ reflect2.Type) (avro.ValDecoder, error) {
	if typ == nil || typ.Kind() != reflect.Pointer {
		return nil, errors.New("decoder: can only unmarshal into pointer")
	}
	return d.api.DecoderOf(d.schema, typ), nil
}

func (d *Decoder) blockConsumed() bool {
	_ = d.block.Peek()
	if d.block.Error == nil {
		d.reader.Error = errors.New("decoder: block has bytes left after its last value")
		return false
	}
	return true
}

// Error returns the last reader error.
func (d *Decoder) Error() error {
	if errors.Is(d.reader.Error, io.EOF) {
		return nil
	}

	return d.reader.Error
}

// Close releases codec resources.
func (d *Decoder) Close() error {
	if c, ok := d.codec.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (d *Decoder) readBlock() int64 {
	_ = d.reader.Peek()
	if errors.Is(d.reader.Error, io.EOF) {
		// There is no next block
		return 0
	}

	count := d.reader.ReadLong()
	if count < 0 {
		d.reader.ReportError("ocf decoder: read block", "negative record count")
		return 0
	}
	size64 := d.reader.ReadLong()
	if size64 < 0 {
		d.reader.ReportError("ocf decoder: read block", "block size is too small")
		return 0
	}
	if size64 > math.MaxInt {
		d.reader.ReportError("ocf decoder: read block", "block size is too big")
		return 0
	}
	if d.maxBlockBytes > 0 && size64 > int64(d.maxBlockBytes) {
		d.reader.ReportError("ocf decoder: read block", "block size is greater than `WithMaxBlockBytes`")
		return 0
	}
	size := int(size64)

	data, err := readBounded(d.reader, size, d.blockBuf)
	if err != nil {
		d.reader.Error = err
		return 0
	}
	d.blockBuf = data

	var sync [16]byte
	d.reader.Read(sync[:])
	if d.reader.Error != nil {
		d.reader.Error = fmt.Errorf("decoder: invalid block: truncated sync marker: %w", d.reader.Error)
		return 0
	}
	if d.sync != sync {
		d.reader.Error = errors.New("decoder: invalid block")
		return 0
	}

	if count == 0 {
		return 0
	}

	decoded, err := d.codec.Decode(data)
	if err != nil {
		d.reader.Error = err
		return 0
	}
	if err = d.checkBlockCount(count, len(decoded)); err != nil {
		d.reader.Error = err
		return 0
	}
	d.block.Reset(decoded)
	d.block.Error = nil

	return count
}

func (d *Decoder) checkBlockCount(count int64, size int) error {
	if d.minValueBytes == 0 {
		if count > d.maxZeroWidthCount {
			return fmt.Errorf("decoder: block count %d exceeds %d for a zero-width schema", count, d.maxZeroWidthCount)
		}
		return nil
	}
	if count > int64(size/d.minValueBytes) {
		return fmt.Errorf("decoder: block count %d does not fit in %d bytes", count, size)
	}
	return nil
}

const blockReadChunkSize = 64 * 1024

// readBounded reads size bytes into buf, growing it chunk by chunk so a lying size cannot force a huge allocation.
func readBounded(r *avro.Reader, size int, buf []byte) ([]byte, error) {
	buf = buf[:0]
	for len(buf) < size {
		n := min(size-len(buf), blockReadChunkSize)
		buf = slices.Grow(buf, n)
		r.Read(buf[len(buf) : len(buf)+n])
		if r.Error != nil {
			return nil, r.Error
		}
		buf = buf[:len(buf)+n]
	}
	return buf, nil
}

type encoderConfig struct {
	BlockLength     int
	BlockSize       int
	CodecName       CodecName
	CodecOptions    codecOptions
	Metadata        map[string][]byte
	Sync            [16]byte
	EncodingConfig  avro.API
	SchemaCache     *avro.SchemaCache
	SchemaMarshaler func(avro.Schema) ([]byte, error)
}

// EncoderFunc represents a configuration function for Encoder.
type EncoderFunc func(cfg *encoderConfig)

// WithBlockLength sets the block length on the encoder.
func WithBlockLength(length int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.BlockLength = length
	}
}

// WithBlockSize sets the maximum uncompressed size of a buffered block before
// it is written and flushed to the underlying io.Writer (after compression).
func WithBlockSize(size int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.BlockSize = size
	}
}

// WithCodec sets the compression codec on the encoder.
func WithCodec(codec CodecName) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecName = codec
	}
}

// WithCompressionLevel sets the compression codec to deflate and
// the compression level on the encoder.
func WithCompressionLevel(compLvl int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecName = Deflate
		cfg.CodecOptions.DeflateCompressionLevel = compLvl
	}
}

// WithZStandardEncoderOptions sets the options for the ZStandard encoder.
func WithZStandardEncoderOptions(opts ...zstd.EOption) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecOptions.ZStandardOptions.EOptions = append(cfg.CodecOptions.ZStandardOptions.EOptions, opts...)
	}
}

// WithZStandardEncoder sets a pre-created ZStandard encoder to be reused.
// This allows sharing a single encoder across multiple OCF encoders for efficiency.
// The caller is responsible for closing the encoder after all OCF encoders are done.
func WithZStandardEncoder(enc *zstd.Encoder) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecOptions.ZStandardOptions.Encoder = enc
	}
}

// WithMetadata sets the metadata on the encoder header.
func WithMetadata(meta map[string][]byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Metadata = meta
	}
}

// WithMetadataKeyVal sets a single key-value pair for the metadata on
// the encoder header.
func WithMetadataKeyVal(key string, val []byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Metadata[key] = val
	}
}

// WithEncoderSchemaCache sets the schema cache for the encoder.
// If not specified, defaults to avro.DefaultSchemaCache.
func WithEncoderSchemaCache(cache *avro.SchemaCache) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.SchemaCache = cache
	}
}

// WithSchemaMarshaler sets the schema marshaler for the encoder.
// If not specified, defaults to DefaultSchemaMarshaler.
func WithSchemaMarshaler(m func(avro.Schema) ([]byte, error)) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.SchemaMarshaler = m
	}
}

// WithSyncBlock sets the sync block.
func WithSyncBlock(sync [16]byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Sync = sync
	}
}

// WithEncodingConfig sets the value encoder config on the OCF encoder.
func WithEncodingConfig(wCfg avro.API) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.EncodingConfig = wCfg
	}
}

// Encoder writes Avro container file to an output stream.
type Encoder struct {
	writer   *avro.Writer
	block    *avro.Writer
	api      avro.API
	schema   avro.Schema
	encoders typeCache[avro.ValEncoder]
	sync     [16]byte

	codec Codec

	blockLength int
	count       int
	blockSize   int

	// Stored for Reset.
	header Header
}

// NewEncoder returns a new encoder that writes to w using schema s.
//
// If the writer is an existing ocf file, it will append data using the
// existing schema.
func NewEncoder(s string, w io.Writer, opts ...EncoderFunc) (*Encoder, error) {
	cfg := computeEncoderConfig(opts)
	schema, err := avro.ParseWithCache(s, "", cfg.SchemaCache)
	if err != nil {
		return nil, err
	}
	return newEncoder(schema, w, cfg)
}

// NewEncoderWithSchema returns a new encoder that writes to w using schema s.
//
// If the writer is an existing ocf file, it will append data using the
// existing schema.
func NewEncoderWithSchema(schema avro.Schema, w io.Writer, opts ...EncoderFunc) (*Encoder, error) {
	return newEncoder(schema, w, computeEncoderConfig(opts))
}

func newEncoder(schema avro.Schema, w io.Writer, cfg encoderConfig) (*Encoder, error) {
	switch file := w.(type) {
	case nil:
		return nil, errors.New("writer cannot be nil")
	case *os.File:
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}

		if info.Size() > 0 {
			reader := avro.NewReader(file, 0, avro.WithReaderConfig(cfg.EncodingConfig))
			h, err := readHeader(reader, cfg.SchemaCache, cfg.CodecOptions)
			if err != nil {
				return nil, err
			}
			if err = skipToEnd(reader, h.Sync); err != nil {
				return nil, err
			}

			writer := avro.NewWriter(w, 0, avro.WithWriterConfig(cfg.EncodingConfig))
			header := Header{Magic: magicBytes, Meta: h.Meta, Sync: h.Sync}
			return newBlockEncoder(writer, h.Schema, h.Codec, header, cfg), nil
		}
	}

	schemaJSON, err := cfg.SchemaMarshaler(schema)
	if err != nil {
		return nil, err
	}

	cfg.Metadata[schemaKey] = schemaJSON
	cfg.Metadata[codecKey] = []byte(cfg.CodecName)
	header := Header{
		Magic: magicBytes,
		Meta:  cfg.Metadata,
	}
	header.Sync = cfg.Sync
	if header.Sync == [16]byte{} {
		_, _ = rand.Read(header.Sync[:])
	}

	codec, err := resolveCodec(cfg.CodecName, cfg.CodecOptions)
	if err != nil {
		return nil, err
	}

	writer := avro.NewWriter(w, 0, avro.WithWriterConfig(cfg.EncodingConfig))
	writer.WriteVal(HeaderSchema, header)
	if err = writer.Flush(); err != nil {
		return nil, err
	}

	return newBlockEncoder(writer, schema, codec, header, cfg), nil
}

func newBlockEncoder(writer *avro.Writer, schema avro.Schema, codec Codec, header Header, cfg encoderConfig) *Encoder {
	return &Encoder{
		writer:      writer,
		block:       avro.NewWriter(nil, 0, avro.WithWriterConfig(cfg.EncodingConfig)),
		api:         cfg.EncodingConfig,
		schema:      schema,
		sync:        header.Sync,
		codec:       codec,
		blockLength: cfg.BlockLength,
		blockSize:   cfg.BlockSize,
		header:      header,
	}
}

func computeEncoderConfig(opts []EncoderFunc) encoderConfig {
	cfg := encoderConfig{
		BlockLength: 100,
		CodecName:   Null,
		CodecOptions: codecOptions{
			DeflateCompressionLevel: flate.DefaultCompression,
		},
		Metadata:        map[string][]byte{},
		EncodingConfig:  avro.DefaultConfig,
		SchemaCache:     avro.DefaultSchemaCache,
		SchemaMarshaler: DefaultSchemaMarshaler,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func (e *Encoder) shouldWriteDataBlock() bool {
	blockLengthReached := e.blockLength != 0 && e.count >= e.blockLength
	blockSizeReached := e.blockSize != 0 && e.block.Buffered() >= e.blockSize
	return blockLengthReached || blockSizeReached
}

// Write v to the internal buffer. This method skips the internal encoder and
// therefore the caller is responsible for encoding the bytes. No error will be
// thrown if the bytes does not conform to the schema given to NewEncoder, but
// the final ocf data will be corrupted.
func (e *Encoder) Write(p []byte) (n int, err error) {
	n, _ = e.block.Write(p)

	e.count++
	if e.shouldWriteDataBlock() {
		if err = e.writerBlock(); err != nil {
			return n, err
		}
	}

	return n, e.writer.Error
}

// Encode writes the Avro encoding of v to the stream.
func (e *Encoder) Encode(v any) error {
	enc, _ := e.encoders.get(v, e.encoderOf)
	mark := e.block.Buffered()
	enc.Encode(reflect2.PtrOf(v), e.block)
	if err := e.block.Error; err != nil {
		e.truncateBlock(mark)
		return err
	}

	e.count++
	if e.shouldWriteDataBlock() {
		if err := e.writerBlock(); err != nil {
			return err
		}
	}

	return e.writer.Error
}

func (e *Encoder) encoderOf(typ reflect2.Type) (avro.ValEncoder, error) {
	return e.api.EncoderOf(e.schema, typ), nil
}

// truncateBlock drops a partially encoded value so a failed Encode leaves the block intact.
func (e *Encoder) truncateBlock(n int) {
	kept := e.block.Buffer()[:n]
	e.block.Reset(nil)
	_, _ = e.block.Write(kept)
	e.block.Error = nil
}

// Flush flushes the underlying writer.
func (e *Encoder) Flush() error {
	if e.count == 0 {
		return nil
	}

	if err := e.writerBlock(); err != nil {
		return err
	}

	return e.writer.Error
}

// Close closes the encoder, flushing the writer and releasing codec resources.
func (e *Encoder) Close() error {
	err := e.Flush()
	if c, ok := e.codec.(io.Closer); ok {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Reset flushes any pending data, resets the encoder to write to a new io.Writer,
// and writes a fresh header with a new sync marker. The schema, codec, and other
// settings are preserved from the original encoder.
// This allows reusing the encoder for multiple files without reallocating buffers.
func (e *Encoder) Reset(w io.Writer) error {
	if err := e.Flush(); err != nil {
		return err
	}

	// Generate new sync marker for the new file.
	_, _ = rand.Read(e.header.Sync[:])
	e.sync = e.header.Sync

	// Reset writer to new output and write header.
	e.writer.Reset(w)
	e.writer.WriteVal(HeaderSchema, e.header)
	if err := e.writer.Flush(); err != nil {
		return err
	}

	e.block.Reset(nil)
	e.count = 0

	return nil
}

func (e *Encoder) writerBlock() error {
	e.writer.WriteLong(int64(e.count))

	b := e.codec.Encode(e.block.Buffer())

	e.writer.WriteLong(int64(len(b)))
	_, _ = e.writer.Write(b)

	_, _ = e.writer.Write(e.sync[:])

	e.count = 0
	e.block.Reset(nil)
	return e.writer.Flush()
}

type ocfHeader struct {
	Schema avro.Schema
	Codec  Codec
	Meta   map[string][]byte
	Sync   [16]byte
}

func readHeader(reader *avro.Reader, schemaCache *avro.SchemaCache, codecOpts codecOptions) (*ocfHeader, error) {
	var h Header
	reader.ReadVal(HeaderSchema, &h)
	if reader.Error != nil {
		return nil, fmt.Errorf("unexpected error: %w", reader.Error)
	}

	if h.Magic != magicBytes {
		return nil, errors.New("invalid avro file")
	}
	schema, err := avro.ParseBytesWithCache(h.Meta[schemaKey], "", schemaCache)
	if err != nil {
		return nil, err
	}

	codec, err := resolveCodec(CodecName(h.Meta[codecKey]), codecOpts)
	if err != nil {
		return nil, err
	}

	return &ocfHeader{
		Schema: schema,
		Codec:  codec,
		Meta:   h.Meta,
		Sync:   h.Sync,
	}, nil
}

func skipToEnd(reader *avro.Reader, sync [16]byte) error {
	for {
		count := reader.ReadLong()
		if errors.Is(reader.Error, io.EOF) {
			return nil
		}
		if count < 0 {
			return errors.New("ocf decoder: negative record count")
		}
		size := reader.ReadLong()
		if size < 0 {
			return errors.New("ocf decoder: negative block size")
		}
		reader.SkipNBytesInt64(size)
		if reader.Error != nil {
			return reader.Error
		}

		var synMark [16]byte
		reader.Read(synMark[:])
		if reader.Error != nil {
			return fmt.Errorf("invalid block: truncated sync marker: %w", reader.Error)
		}
		if sync != synMark {
			return errors.New("invalid block")
		}
	}
}

func defaultMarshalSchema(schema avro.Schema) ([]byte, error) {
	return []byte(schema.String()), nil
}

func fullMarshalSchema(schema avro.Schema) ([]byte, error) {
	return json.Marshal(schema)
}
