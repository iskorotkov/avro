# Memory Allocation Controls

## Summary

This library includes configuration options to prevent denial-of-service attacks through excessive memory allocation when processing untrusted Avro data. Users should configure appropriate limits based on their use case.

## Required Configuration

Users processing untrusted Avro data **must** configure appropriate size limits using the `Config` struct. Adjust these values based on your legitimate data requirements. Lower values provide better protection but may reject valid large datasets.

```go
cfg := avro.Config{
    MaxByteSliceSize:  102_400, // 100 kiB - limit for bytes/string types
    MaxSliceAllocSize: 10_000,  // 10k - limit for array lengths
    MaxMapAllocSize:   10_000,  // 10k - limit for map sizes
}
api := cfg.Freeze()

// Now use the frozen API for untrusted data, e.g.:
err := api.Unmarshal(schema, untrustedData, &result)
```

Alternatively update the defaults globally:

```go
avro.DefaultConfig = avro.Config{
    MaxByteSliceSize:  102_400, // 100 kiB - limit for bytes/string types
    MaxSliceAllocSize: 10_000,  // 10k - limit for array lengths
    MaxMapAllocSize:   10_000,  // 10k - limit for map sizes
}.Freeze()

// Now use the Avro functionality for untrusted data, e.g.:
decoder, err := avro.NewDecoder(schema, untrustedReader)
```

## Configuration Guidelines

1. **MaxByteSliceSize**: Controls the maximum size of individual `bytes` or `string` values. Default: 1 MiB. Set to -1 to disable (not recommended for untrusted input).

2. **MaxSliceAllocSize**: Controls the maximum length of an individual array. Default: unlimited. Set this to a reasonable value based on your expected data size.

3. **MaxMapAllocSize**: Controls the maximum capacity of an individual map. Default: unlimited. Set this to a reasonable value based on your expected data size.

## Decoding untrusted OCF (Object Container Files)

OCF files contain a sequence of *blocks*. Each block declares its compressed size in a header, then carries a compressed payload (deflate, snappy, or zstandard) followed by a 16-byte sync marker. Two separate caps protect the decoder against hostile or corrupt input:

```go
dec, err := ocf.NewDecoder(
    untrustedReader,
    ocf.WithMaxBlockBytes(16 << 20),             // compressed cap, 16 MiB
    ocf.WithMaxDecompressedBlockBytes(64 << 20), // decompressed cap, 64 MiB
)
```

| Option | What it bounds | Defends against |
| --- | --- | --- |
| `WithMaxBlockBytes(n)` | the **compressed** block size declared in the block header, checked before any allocation | a header that claims a huge block (forced pre-allocation OOM) |
| `WithMaxDecompressedBlockBytes(n)` | the **decompressed** output produced by the codec | zip-bomb-style amplification (small block expands to gigabytes) |

Both caps default to unlimited for backward compatibility. **For untrusted input, both must be set.** A compressed cap alone is not sufficient: deflate, snappy, and zstandard all support amplification ratios well past 1000:1.

### Codec coverage

- **`null`** — no compression, no amplification. The decompressed cap is implicitly enforced by the compressed cap.
- **`deflate`** — the cap is enforced by reading the inflater through an `io.LimitReader` of `n+1` bytes. Streams that would expand past the cap error out without growing the buffer further.
- **`snappy`** — the cap is checked against the varint length header *before* `snappy.Decode` allocates the destination buffer. A header claiming N bytes of decoded data is rejected when N exceeds the cap.
- **`zstandard`** — for a decoder the library constructs itself, `zstd.WithDecoderMaxMemory` is installed at construction time with `max(n, 8 MiB)`, so an oversized frame is rejected *during* decoding once it passes that bound, and the exact cap `n` is checked on the output. The 8 MiB floor exists because the zstd memory limit also caps the frame window, and RFC 8878 asks decoders to accept 8 MiB windows (streaming writers such as zstd-jni declare windows of several MiB even for small blocks); a hostile block can therefore make the decoder allocate on the order of `2 × max(n, 8 MiB)` before it is rejected, because output of unknown size is grown by appending and checked once per 128 KiB zstd block. **A shared decoder supplied via `WithZStandardDecoder` does not get this protection:** the library cannot install `WithDecoderMaxMemory` on a decoder it does not own, so `DecodeAll` materializes the *entire* decompressed frame in memory and the OCF-level cap only rejects it afterwards. That post-decode check stops the oversized data from being *used*, but it does **not** prevent the allocation — a small block can still OOM the process. If you process untrusted input with a shared decoder, you **must** construct that decoder yourself with `zstd.WithDecoderMaxMemory` set, or simply not share a decoder.

Every codec rejects an oversized block with an error wrapping `ocf.ErrDecompressedBlockTooLarge`, so callers can match it with `errors.Is`.

### Block structure checks

The decoder also rejects blocks that are internally inconsistent, independent of the caps above:

- a negative record count or block size, or a missing or mismatched sync marker;
- a record count that cannot fit in the decompressed bytes, given the smallest possible encoding of the schema;
- for schemas that encode to zero bytes (such as `"null"` or an empty record), a record count above `WithMaxZeroWidthBlockCount(n)` (default `ocf.DefaultMaxZeroWidthBlockCount`, 2^24);
- a block that ends in the middle of a value (`io.ErrUnexpectedEOF`), or that still holds bytes after its declared number of values.

Blocks with a record count of zero are skipped without being decompressed.

### Reading OCF metadata (header)

The OCF header itself contains a `map<string, bytes>` of arbitrary metadata, parsed *before* any data block — and therefore before any of the block-level caps above can apply. It is bounded only by `MaxByteSliceSize` and `MaxMapAllocSize` from the value-decoder config. `MaxMapAllocSize` defaults to effectively unbounded (`1<<48` on 64-bit platforms), so a hostile header declaring a huge entry count can OOM the decoder before a single block is read. **For untrusted input you must** pass `WithDecoderConfig` a frozen `avro.Config` that sets `MaxMapAllocSize` (and `MaxByteSliceSize`) to sane values, in addition to the OCF-specific caps above.
