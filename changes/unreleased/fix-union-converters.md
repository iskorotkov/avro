## Security

- Decoding a `null` union value into a nullable `[]byte` (or other slice) left a slice with a nil data pointer but its old length, so indexing it crashed; the whole slice is now cleared.
- Decoding a nullable union into a struct value (not a pointer) treated the struct's first field as a pointer, corrupting memory or panicking depending on the input; such targets are now decoded in place and set to the zero value on `null`.
- A nullable union decode converter returning a small value such as `int64(5)` for a `*int64` target left the pointer aimed at Go's shared read-only data, so writing through it crashed the process; converter results are now copied into a fresh value.
- Decoding a union into a struct value whose pointer implements `UnionConverter` wrote a fresh pointer over the struct's first field and called `FromAny` on a throwaway copy; such values are now decoded in place.
- Map union codecs now accept only string-keyed maps of empty interfaces; maps of non-empty interfaces, whose memory layout differs, now return an error instead of being misread.

## Behaviour changes

- Encoding a union from `map[string]any` now runs the converter registered for the selected branch type (for example `avro.Long`); a converter registered for `avro.Union` runs only when the branch type has none, and an error from the branch converter is returned without falling back. Previously only the `avro.Union` converter ran. Users registering both kinds of converters get different results without a compile error.
- An `any` value holding a `map[string]any` union is no longer passed to the `avro.Union` encode converter as a whole map before the branch value is converted.
- A nullable union decode converter must return a value assignable to the target (a `T` is boxed for a `*T` target); other results now fail with `avro: type converter returned X, expected Y` instead of being stored unchecked.
- Decoding a union into `any` still runs only the `avro.Union` converter, not branch-type converters; this is now documented in the README.
