## Security

- Truncated input is no longer silently accepted as a partial value with zero-filled fields; see Breaking changes.

## Breaking changes

- `Unmarshal` returns `*avro.ShortBufferError` wrapping `io.ErrUnexpectedEOF` when the input ends before the value is complete: inside a varint, between record fields, inside a block or union, and for empty input when the schema needs bytes. Such input previously decoded to a partial value with a `nil` error. Bytes left after a complete value are still ignored.
- `Decoder.Decode` returns `io.ErrUnexpectedEOF` when the stream ends inside a value; it still returns `io.EOF` when the stream ends before a value.
- `soe` `DecodeUnverified` of an empty payload now fails with `io.ErrUnexpectedEOF` instead of returning a zero value.

## Behaviour changes

- Record field errors are `*avro.SemanticError` values with a dotted `Field` path; messages and `errors.Is` matching are unchanged.
- Array encode errors name the slice type once instead of twice (`L: []T: ...`).

## Added

- `AppendMarshal(dst, schema, v)` and the `AppendMarshaler` interface encode into a caller-owned buffer without allocating once the codec is cached; `AppendMarshalWith(api, ...)` works with any `API`.
- `Config.DisallowUnknownFields` rejects struct and record schemas whose fields do not match. The whole Go type is checked when the codec is built, so mismatches inside empty arrays, empty maps or `null` union branches also fail; errors wrap `avro.ErrUnknownField`.
- `Config.AliasInput` makes `Unmarshal` return strings and byte slices that share memory with the input; streaming decoders always copy.
- `avro.SemanticError` and `avro.ShortBufferError` error types.
