## Breaking changes

- `API.DecoderOf`, `API.EncoderOf`, `API.TypeOf`, `API.NamesOf`, `TypeResolver.Name` and `TypeResolver.Type` take or return `reflect.Type` instead of `reflect2.Type`; replace `reflect2.TypeOf(v)` with `reflect.TypeOf(v)` or `reflect.TypeFor[T]()`.
- The `github.com/modern-go/reflect2` dependency is removed.

## Behaviour changes

- `Reader.ReadVal` reports an error for a `nil` target instead of panicking.
- Pointer-receiver `MarshalText`, `AppendText` and `UnmarshalText` methods no longer see reused or stack scratch storage, so receivers they keep stay valid.
- Encoded bytes are unchanged; a golden test pins them to the previous reflect2-based output.

## Added

- Native map encoding for `time.Time` keys and values, pointer keys and named key/value types, and native decoding of string-keyed maps with primitive, pointer, slice or `any` values, with fewer allocations.
