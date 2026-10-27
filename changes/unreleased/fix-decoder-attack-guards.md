## Security

- Recursive records (linked lists, trees) nest at most `Config.MaxDepth` levels (default 1000; zero or negative selects the default) when decoding, skipping, reading with `Reader.ReadNext` and encoding, so deep payloads and cyclic Go values fail instead of exhausting the stack.
- Array and map block counts must fit in the remaining input when its length is known (byte slices, `*bytes.Reader`, `*bytes.Buffer`, `*strings.Reader`, OCF blocks); slices and maps are pre-allocated for at most 4096 items, so forged counts on streams cannot allocate ahead of the data.
- Arrays of items without wire representation (`null`, empty records) are capped at 1,048,576 items per decoded value.
- Decoding stops at the first error, so truncated or corrupt input fails fast.
- Skipped fields and `Reader.ReadNext` obey the same limits as typed decoding.

## Behaviour changes

- `Unmarshal`, `Decoder.Decode` and `Reader.ReadNext` return an error wrapping `io.ErrUnexpectedEOF` when the input ends inside a value; previously they returned `nil` with a partially filled value. `Decoder.Decode` still returns `io.EOF` at a clean value boundary.
- `soe` `Codec.DecodeUnverified` and `TypedCodec.DecodeUnverified` fail with `io.ErrUnexpectedEOF` when the payload after the header is empty or truncated instead of returning a zero or partial value.
- `Reader.ReadBytes` and `Reader.ReadString` return `nil` and `""` when they fail instead of a partially read value.
- `Config.MaxSliceAllocSize` and `Config.MaxMapAllocSize` are budgets per decoded value, summed over all arrays (or maps) it contains including skipped fields, instead of per array (or map).
- `Config.MaxDepth` also applies when encoding: cyclic values return an error instead of overflowing the stack.

## Added

- `Config.MaxDepth`.
- Error sentinels for `errors.Is`: `ErrMaxDepth`, `ErrMaxSliceAllocSize`, `ErrMaxMapAllocSize`, `ErrMaxByteSliceSize`, `ErrTooManyZeroWidthItems` and `ErrBlockCountExceedsInput` (which wraps `io.ErrUnexpectedEOF`).
