## Security

- `ocf`: the zstandard decoder now accepts frames whose window is larger than a small `WithMaxDecompressedBlockBytes` cap, so it may allocate up to `max(cap, 8 MiB)` before rejecting a hostile block (see SECURITY.md). The exact cap is still enforced on the output.
- `ocf`: process-wide shared zstandard decoders are capped at 8 distinct memory limits; further limits get a decoder owned by the OCF decoder and released by `Decoder.Close`.

## Behaviour changes

- `ocf`: the decoder validates block structure more strictly. It rejects a record count that cannot fit in the block, more than `WithMaxZeroWidthBlockCount` records (default 2^24) for zero-width schemas such as `"null"`, a block ending mid-value (`io.ErrUnexpectedEOF`), a block with bytes left after its last value, and truncated or mismatched sync markers. Blocks with a record count of zero are skipped instead of ending iteration. The leftover-bytes check matches Apache Avro Java (`DataFileStream.hasNext` throws "Block read partially"); skipping empty blocks is more lenient than Java, which stops reading at one.
- `ocf`: built-in codecs (`DeflateCodec`, `SnappyCodec`, `ZStandardCodec`) reuse internal buffers. A slice returned by `Encode` or `Decode` is only valid until the next call to the same method, and a codec must not be used concurrently. `Decoder` and `Encoder` already use one codec each and are unaffected.
- `ocf`: zstandard codecs created without options share process-wide default encoder and decoder instances.
- `ocf`: decompressed-size errors carry new messages that wrap `ErrDecompressedBlockTooLarge`.
- `soe`: `Codec.Encode` and `AppendEncode` frame the header and value in one pooled writer; APIs that cannot back an `avro.Writer` fall back to `Marshal`.

## Added

- `ocf.ErrDecompressedBlockTooLarge`, wrapped by every built-in codec when a block exceeds `WithMaxDecompressedBlockBytes`.
- `ocf.WithMaxZeroWidthBlockCount` and `ocf.DefaultMaxZeroWidthBlockCount` to configure the record-count cap for zero-width schemas.
