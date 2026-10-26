# Memory Allocation Controls

## Summary

This library includes configuration options to prevent denial-of-service attacks through excessive memory allocation when processing untrusted Avro data. Users should configure appropriate limits based on their use case.

## Recommended Configuration

Safe limits apply by default (see the guidelines below). Users processing untrusted Avro data should tighten them to match their legitimate data requirements using the `Config` struct. Lower values provide better protection but may reject valid large datasets.

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

1. **MaxByteSliceSize**: Controls the maximum size of individual `bytes` or `string` values. Default: 1 MiB. Zero or negative values select the default; the limit cannot be disabled, but can be raised up to the platform allocation limit. A length larger than the remaining input fails with an error wrapping `io.ErrUnexpectedEOF` before anything is allocated.

2. **MaxSliceAllocSize**: Controls the total number of array elements in one decoded value, summed over all arrays it contains (including skipped fields). Default: 67,108,864. Zero or negative values select the default.

3. **MaxMapAllocSize**: Controls the total number of map entries in one decoded value, summed over all maps it contains (including skipped fields). Default: 67,108,864. Zero or negative values select the default.

4. **MaxDepth**: Controls how many values of recursive record types (records that can contain themselves, such as linked lists or trees) may nest inside each other. Default: 1000. Zero or negative values select the default. Deeper data fails to decode, and cyclic Go values fail to encode. Non-recursive schemas are not affected because their nesting is fixed by the schema. `Reader.ReadNext` counts every nested record.

## Built-in Protections

These checks always apply and need no configuration:

- An array or map block count must fit in the remaining input, assuming each item takes the minimum number of bytes its schema allows. The remaining input is known when decoding a byte slice or reading from an `io.Reader` that has a `Len() int` method, such as `bytes.Reader`, `bytes.Buffer` and OCF data blocks.
- Slices and maps are pre-allocated for at most 4096 items and grow as items are actually decoded, so a forged count on a stream cannot allocate ahead of the data.
- Arrays whose items have no wire representation (such as `null` or empty records) are limited to 1,048,576 items per decoded value.
- The limits above also apply to fields skipped because the target struct lacks them, and to `Reader.ReadNext`, `Reader.ReadArrayCB` and `Reader.ReadMapCB`.

## Truncated Input

`Unmarshal` and `Decoder.Decode` return an error wrapping `io.ErrUnexpectedEOF` when the input ends inside a value. Earlier versions returned `nil` and left the target partially filled. `Decoder.Decode` still returns `io.EOF` when the stream ends cleanly between values.

## Decoding untrusted OCF (Object Container Files)

OCF files contain a sequence of *blocks*. Each block declares its compressed size in a header, then carries a compressed payload (deflate, snappy, or zstandard) followed by a 16-byte sync marker. Two separate caps protect the decoder against hostile or corrupt input:

```go
dec, err := ocf.NewDecoder(
    untrustedReader,
    ocf.WithMaxBlockBytes(16 << 20),             // compressed cap, 16 MiB
    ocf.WithMaxDecompressedBlockBytes(32 << 20), // decompressed cap, 32 MiB
)
```

| Option | What it bounds | Defends against |
| --- | --- | --- |
| `WithMaxBlockBytes(n)` | the **compressed** block size declared in the block header, checked before any allocation | a header that claims a huge block (forced pre-allocation OOM) |
| `WithMaxDecompressedBlockBytes(n)` | the **decompressed** output produced by the codec | zip-bomb-style amplification (small block expands to gigabytes) |

Both caps default to 64 MiB; zero or negative values select the default. Consider lowering both when decoding untrusted input. A compressed cap alone is not sufficient: deflate, snappy, and zstandard all support amplification ratios well past 1000:1.

### Codec coverage

- **`null`** — no compression, no amplification. The decompressed cap is implicitly enforced by the compressed cap.
- **`deflate`** — the cap is enforced by a `LimitReader` wrapping `io.ReadAll`. Streams that would expand past the cap error out without growing the buffer further.
- **`snappy`** — the cap is checked against the varint length header *before* `snappy.Decode` allocates the destination buffer. A header claiming N bytes of decoded data is rejected when N exceeds the cap.
- **`zstandard`** — for a decoder the library constructs itself, the cap is installed as `zstd.WithDecoderMaxMemory` at construction time, so an oversized frame is rejected *during* decoding before it is fully materialized. **A shared decoder supplied via `WithZStandardDecoder` does not get this protection:** the library cannot install `WithDecoderMaxMemory` on a decoder it does not own, so `DecodeAll` materializes the *entire* decompressed frame in memory and the OCF-level cap only rejects it afterwards. That post-decode check stops the oversized data from being *used*, but it does **not** prevent the allocation — a small block can still OOM the process. If you process untrusted input with a shared decoder, you **must** construct that decoder yourself with `zstd.WithDecoderMaxMemory` set, or simply not share a decoder.

### Reading OCF metadata (header)

The OCF header itself contains a `map<string, bytes>` of arbitrary metadata, parsed *before* any data block — and therefore before any of the block-level caps above can apply. It is bounded by `MaxByteSliceSize` and `MaxMapAllocSize` from the value-decoder config, and its map is pre-allocated for at most 4096 entries. For untrusted input, pass `WithDecoderConfig` a frozen `avro.Config` that sets `MaxMapAllocSize` (and `MaxByteSliceSize`) to tight values, in addition to the OCF-specific caps above.
