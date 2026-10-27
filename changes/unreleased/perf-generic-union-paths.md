## Behaviour changes

- Encoding a union from `map[string]any` whose value is `nil` for a non-null branch (for example `{"string": nil}`) now returns `avro: nil value for non-null union branch string` instead of panicking.

## Added

- Faster generic encoding and decoding with `map[string]any`, `[]any` and `any`: union branches reuse cached codecs, union map keys are precomputed, plain primitives are decoded without scratch allocations, and type converter lookups are skipped when no converters are registered. Measured with `go test -run='^$' -bench='UnionGeneric|SuperheroGeneric' -benchtime=200ms -count=5` against the benchmark-only base, generic union encode is about 78% faster with 90% fewer allocations, generic union decode about 49% faster, and superhero generic decode about 27% faster.
