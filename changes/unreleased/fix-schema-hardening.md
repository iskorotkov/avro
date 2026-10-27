# fix/schema-hardening

## Security

- Schemas from untrusted sources are bounded by parser limits: `fixed` sizes above `avro.MaxFixedSize()` (default 1 MiB) fail to parse, and `decimal` logical types with a precision above `avro.MaxDecimalPrecision()` (default 1000) are read as their underlying `bytes`/`fixed` type. Change them at any time, including concurrently with parsing, with `avro.SetMaxFixedSize` and `avro.SetMaxDecimalPrecision`; `<= 0` disables a limit.
- A `fixed` size that does not fit in `int` (on 32-bit platforms) fails to parse instead of wrapping around.
- `SchemaCompatibility.Resolve` memoizes record pairs, so recursive schemas no longer overflow the stack and DAG-shaped schemas resolve in linear time.
- `ocf.NewDecoder`, appending with `ocf.NewEncoder`, and every `registry.Client` response parse into a private schema cache that reads from `avro.DefaultSchemaCache` but never writes to it.
- `Config.MaxCodecCacheSize` bounds the encoder and decoder caches (default 0, unbounded); a full cache evicts an arbitrary codec.

## Behaviour changes

- Parsing never overwrites a name a schema cache already resolves (first definition wins). Re-parsing an evolved schema, e.g. `avro.Parse` of version 2 of `com.app.User` after version 1, returns version 2, but `avro.DefaultSchemaCache` and later schemas that reference `com.app.User` by name keep version 1. A parse that redefines any cached name, including only its defaults, aliases or logical type, adds none of its names. Use a fresh `avro.SchemaCache` with `avro.ParseWithCache` per schema version.
- `avro.SchemaCache.AddAll` is unchanged: it still overwrites existing names (last definition wins).
- Named types from OCF headers and registry responses no longer appear in `avro.DefaultSchemaCache`. Pass `ocf.WithDecoderSchemaCache(avro.DefaultSchemaCache)` or `registry.WithNamedSchemaCache(avro.DefaultSchemaCache)` to restore sharing for trusted sources.
- A registry schema that references a named type by name only resolves it from another response when the client shares a cache: `GetSchema(1)` followed by `GetSchema(2)`, where schema 2 uses a type defined only in schema 1, now fails with `avro: unknown type` unless the client uses `registry.WithNamedSchemaCache`.
- Encoder and decoder caches take field defaults into account, so schemas differing only in defaults no longer share codecs (hamba/avro#449). The first `CacheFingerprint` call per schema costs more; later calls are unchanged.
- Empty unions (`[]`) parse, as in the Avro spec and Apache Avro Java: same canonical form and fingerprints, every default rejected, and encoding or decoding any value fails.
- Decoding a `bytes` decimal into a `big.Rat` value (not a pointer) now sets the value instead of leaving it zero.

## Added

- `avro.NewSchemaCache(parent)`, a cache that reads from its parent without writing to it.
- `avro.MaxFixedSize`, `avro.SetMaxFixedSize`, `avro.DefaultMaxFixedSize`, `avro.MaxDecimalPrecision`, `avro.SetMaxDecimalPrecision`, `avro.DefaultMaxDecimalPrecision`.
- `Config.MaxCodecCacheSize`.
- `registry.WithNamedSchemaCache`.
