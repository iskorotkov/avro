# Custom types on struct fields

Status: proposal (design only, no code yet). Related: hamba/avro#462 (ISSUES.md), iskorotkov/avro#45 (closed, `json` logical type), twmb/avro `custom_type.go`. See also [schema-for.md](schema-for.md), whose type mapping should consult registered custom types.

## Problem

Users want to map their own Go types onto Avro schema nodes: `Money` on a `long` with `logicalType: "money"`, `uuid.UUID` on a `fixed(16)`, `json.RawMessage` on `bytes` with `logicalType: "json"`, a domain `Timestamp` on `timestamp-micros`.

Today the only hook is `TypeConverters` (`typeconverter.go`, `API.RegisterTypeConverters`). It is keyed by `(Type, LogicalType)` and runs only where the codec sees an `any`:

- `codec_union.go` (~184, 247, 262, 463): union encode/decode into interface or map-union targets.
- `codec_dynamic.go` (32, 69): `efaceDecoder` and `interfaceEncoder`.

A concrete struct field (`Price Money`) never reaches those paths: `decoderOfType`/`encoderOfType` in `codec.go` compile a native codec from `(schema, reflect type)` and either succeed by kind conversion (silently ignoring the logical type) or fail with "is unsupported". The converters also box every value through `any`, which is incompatible with the zero-alloc compiled codecs this fork is built around.

Two further facts shape the design:

- Unknown logical types (e.g. `json`, `money`) are left in `Props()["logicalType"]` by `schema_parse.go`; `getLogicalType` returns `""` for them.
- `CacheFingerprint()` ignores unknown logical types: `{"type":"bytes","logicalType":"json"}` and `"bytes"` currently share a fingerprint (verified), while built-in ones like `timestamp-millis` do not.

## Goals

- Let users bind a concrete Go type to Avro schema nodes for struct fields, map/array elements, pointers and union branches with concrete targets.
- Dispatch once, at codec build time; zero runtime cost for schemas and types that do not match.
- Type-safe generic constructor whose codec does no boxing for primitive Avro types.
- Additive, opt-in, backwards compatible; existing `TypeConverters`, `TextMarshaler` and logical type behavior unchanged when nothing is registered.

## Non-goals

- Changing `TypeConverters` or the `any`/generic decode paths (`ReadNext`, `efaceDecoder`).
- Schema generation from Go types (this library has no `SchemaFor`).
- `avrogen` annotations (#462 proper); this proposal is the runtime prerequisite, gen support is a follow-up.
- Runtime fall-through (`ErrSkipCustomType`); dispatch is static, so there is nothing to fall through to.

## Comparison with twmb/avro

| Aspect | twmb/avro | Proposed |
|---|---|---|
| Registration | per schema, `SchemaOpt` to `Parse` | per config (`Config.CustomTypes`), like `TypeConverters` |
| Match criteria | `LogicalType`, `AvroType` at parse; `GoType` checked at encode time | `AvroType`, `LogicalType`, optional `Name`, and `GoType`, all at codec build time |
| Dispatch | runtime chain of `func(any) (any, error)` with `ErrSkipCustomType` | static: matched node gets a dedicated `ValEncoder`/`ValDecoder` |
| Generic helper | `NewCustomType[G, A](logical, enc, dec)`, A must be primitive | `NewCustomType[G, A](avroType, logical, enc, dec)`, A is anything this library can already codec for the node |
| Values | boxed `any` both ways | typed via `unsafe.Pointer` in the generic form; boxed only in the struct form |
| Wildcard matching | yes (empty criteria matches every node) | no; `AvroType` is required |

We borrow the shape (`CustomType` struct plus `NewCustomType[G, A]`) and the "custom beats built-in logical type" rule, and drop runtime dispatch because it would put a closure chain on every value.

## Proposed API

New file `custom_type.go`:

```go
// CustomType binds a Go type to Avro schema nodes for encoding and decoding.
type CustomType struct {
	AvroType    Type                                  // Required Avro type of the matched node.
	LogicalType LogicalType                           // Optional logical type, built-in or unknown.
	Name        string                                // Optional full name for named types (fixed, enum, record).
	GoType      reflect.Type                          // Required Go type the codec is built for.
	Encode      func(v any, schema Schema) (any, error) // Converts a GoType value to an Avro-native value.
	Decode      func(v any, schema Schema) (any, error) // Converts an Avro-native value to a GoType value.

	build func(schema Schema) (customCodec, error) // Set by NewCustomType for the typed fast path.
}

// NewCustomType returns a CustomType that converts between G and an Avro-native type A without boxing.
func NewCustomType[G, A any](
	avroType Type,
	logicalType LogicalType,
	encode func(G, Schema) (A, error),
	decode func(A, Schema) (G, error),
) CustomType

// CustomTypes is an immutable, validated set of custom types for a Config.
type CustomTypes struct{ /* index by GoType rtype, then match list */ }

// NewCustomTypes validates the given custom types and indexes them by Go type.
func NewCustomTypes(cts ...CustomType) (*CustomTypes, error)
```

`Config` gains one field (a pointer, so `Config` stays comparable):

```go
// CustomTypes binds Go types to schema nodes; nil disables the feature.
CustomTypes *CustomTypes
```

For `DefaultConfig` users, a package-level helper mirrors `RegisterTypeConverters`:

```go
// RegisterCustomTypes adds custom types to DefaultConfig and must be called before first use.
func RegisterCustomTypes(cts ...CustomType) error
```

The `API` interface is not extended (adding a method would break external implementers). Example:

```go
// Money is stored as cents on a long annotated with logicalType money.
var moneyType = avro.NewCustomType[Money, int64](avro.Long, "money",
	func(m Money, _ avro.Schema) (int64, error) { return m.Cents(), nil },
	func(c int64, _ avro.Schema) (Money, error) { return MoneyFromCents(c), nil },
)

cts, err := avro.NewCustomTypes(moneyType)
api := avro.Config{CustomTypes: cts}.Freeze()
```

Validation in `NewCustomTypes`: `AvroType` and `GoType` non-empty; `Encode` or `Decode` set; no two entries with identical `(GoType, AvroType, LogicalType, Name)`; `A` must be a type `encoderOfType`/`decoderOfType` can build for `AvroType` (checked lazily at build since it needs a schema). A nil `Encode` makes encoding that type an `errorEncoder` with a clear message (likewise for `Decode`).

## Dispatch rules and precedence

A custom type matches a `(schema node, Go type)` pair when all hold:

1. `typ.RType()` equals `GoType` exactly (no implements/convertible checks; register `*G` separately if needed).
2. `schema.Type()` equals `AvroType` after resolving `Ref`.
3. If `LogicalType` is set, it equals `getLogicalType(schema)` or, for unknown types, `schema.(PropertySchema).Prop("logicalType")`.
4. If `Name` is set, it equals the named schema's `FullName()`.

Among matches the most specific wins (Name > LogicalType > AvroType only); a tie is impossible after validation.

Precedence inside `decoderOfType`/`encoderOfType`:

1. Custom type (new, first).
2. `TextMarshaler`/`TextAppender` (`createDecoderOfMarshaler`/`createEncoderOfMarshaler`).
3. Interface/eface handling, where existing `TypeConverters` keep running unchanged.
4. Built-in native and logical type codecs (`codec_native.go`), records, unions, etc.

Consequences:

- A custom type for `time.Time` on `timestamp-micros` overrides the built-in logical codec; users opt into this explicitly.
- A type that implements `TextMarshaler` and has a custom type uses the custom type.
- `TypeConverters` and custom types never compete: converters see `any` values, custom types see concrete Go types. Registering both for the same logical type is allowed and documented.
- A value of type `G` stored in an `any` field is not matched; it goes through the existing interface path (and `TypeConverters`). See open questions.

## Codec integration and caching

New file `codec_custom.go` holds `createDecoderOfCustom(d, schema, typ)` and `createEncoderOfCustom(e, schema, typ)`. The hook is one guarded call at the top of `decoderOfType` and `encoderOfType` in `codec.go`:

```go
// Custom types are checked first and cost a nil check when none are registered.
if ct := d.cfg.customTypes; ct != nil {
	if dec := createDecoderOfCustom(d, ct, schema, typ); dec != nil {
		return dec
	}
}
```

Because record fields (`codec_record.go:111,184`), map/array elements, pointers (`codec_ptr.go`) and union branches all recurse through these two functions, every concrete position is covered by this single hook.

Codec shapes:

- Typed form (`NewCustomType`): the inner codec for `A` is built with `encoderOfType(e, schema, reflect2.TypeOf[A])` with the custom hook suppressed for that node (a flag on the context) to avoid self-recursion. Encode is `a, err := enc(*(*G)(ptr), schema); inner.Encode(unsafe.Pointer(&a), w)`; decode reads into a stack `A`, calls `dec`, stores `*(*G)(ptr) = g`. `A` may be a primitive, `[]byte`, a struct or `map[string]any`, so records and fixed types work without a separate path.
- Struct form (`CustomType` with `any` funcs): boxes through `reflect2.Type.UnsafeIndirect`, runs `Encode`, then `w.WriteVal(schema, out)`; decode uses the eface decoder and `UnsafeSet`. This is the slow, flexible path, as today's converters.
- Errors from `Encode`/`Decode` are set on `w.Error`/`r.Error`, matching existing codecs.

Caching: codecs stay cached in `frozenConfig.decoderCache`/`encoderCache` under `cacheKey{CacheFingerprint, rtype}`. Since `CustomTypes` is fixed at `Freeze`, cached codecs never go stale. `RegisterCustomTypes` on `DefaultConfig` clears both caches (`sync.Map.Clear`) and is documented as init-time only.

Fingerprint gap: because unknown `logicalType` props are not part of `CacheFingerprint()`, a struct containing `Money` used with a `long` schema and a `long`+`money` schema would collide on one cache entry. Fix: `cacheFingerprinter.CacheFingerprint` mixes in a hash of unknown `logicalType` values found in the schema tree, computed once and memoized like `externalRefsIdentity`. Schemas without unknown logical types keep their current value, and `Fingerprint()` (used for schema registries) is untouched.

## Performance

- Unaffected configs: one nil pointer check per `decoderOfType`/`encoderOfType` call, which happens only at codec build time; steady-state encode/decode is byte-for-byte the same compiled codec.
- Configs with custom types, unmatched nodes: one map lookup by rtype at build time.
- Matched nodes, typed form: one indirect call to the user function plus the native codec for `A`; zero allocations for scalar `A`, verified by `testing.AllocsPerRun`.
- Matched nodes, struct form: one interface boxing per value each way, same as today's `TypeConverters`.
- `CacheFingerprint` change: computed once per schema and memoized; no per-call cost.

Benchmarks to add in `bench_custom_type_test.go`: struct with one `Money` field vs the same struct with `int64`, typed and struct forms, encode and decode, guarded by the existing benchmark comparison workflow.

## Compatibility

- No behavior change unless `Config.CustomTypes` is set or `RegisterCustomTypes` is called.
- `Config` remains comparable (pointer field). `API` interface unchanged.
- `CacheFingerprint()` changes only for schemas containing unknown logical types; it is a cache identity, not a wire or registry identity, and existing codecs ignore those props, so only cache-entry splitting is observable.
- Wire format never changes; custom types are a Go-side mapping only.
- PR #45's `json` logical type becomes expressible without core changes: `NewCustomType[json.RawMessage, []byte](avro.Bytes, "json", ...)`.

## Alternatives considered

- Extend `TypeConverters` into concrete paths: requires boxing every value and a runtime map lookup per value; breaks the zero-alloc guarantee.
- Per-schema registration like twmb (`Parse(..., WithCustomType)`): fits twmb's schema-owned codecs, but here codecs live in the config cache and schemas are shared across configs; per-schema state would also need to enter the cache key.
- Interface on the Go type (`AvroMarshaler`/`AvroUnmarshaler`, like `TextMarshaler`): simple and already half-present, but cannot target third-party types (`uuid.UUID`, `decimal.Decimal`) and cannot see the schema node's logical type. Could be added later on top of the same hook.
- Runtime fall-through with `ErrSkipCustomType`: unneeded once `GoType` is matched at build time; omitted to keep codecs static.
- Retain unknown logical types as `LogicalSchema` in the parser: changes `Fingerprint()` and schema printing for existing users; rejected.

## Open questions

1. Should custom types also apply to `any` targets (generic decode, union into `any`) by producing `G`, effectively auto-registering a `TypeConverter`? Proposed: opt-in flag later, not v1.
2. Should union encoding resolve a branch from a `G` value via `TypeResolver`, e.g. `RegisterCustomTypes` also calling `Register` with the logical-type name? Needed for `*G` in `[null, long]` (works via ptr codec) vs `G` in a multi-branch union held as `any`.
3. Should `GoType` match also cover `*G` automatically, given the ptr codec already handles nil?
4. Is a `Name`-only match (any Go type for a named fixed) worth supporting, or must `GoType` always be set?
5. Schema evolution: when reader and writer logical types differ, which side's logical type should the match use? Proposed: the reader schema, consistent with resolved-schema codecs.

## Test plan

- `custom_type_test.go`: validation errors in `NewCustomTypes`; `NewCustomType` inference; nil `Encode`/`Decode` behavior.
- `codec_custom_test.go`, table-driven over position: top-level, record field, nested record, array element, map value, `*G` in `[null, T]`, union branch with concrete target; for long, int, bytes, string, fixed (`uuid`), record-backed `A`.
- Matching: built-in logical type override (`time.Time` on `timestamp-micros`), unknown logical type (`json`, `money`), `Name` match on fixed, precedence over `TextMarshaler`, no match falls back unchanged.
- Cache: same struct with `long` and `long`+`money` schemas yields different codecs; `DisableCaching` path; `RegisterCustomTypes` clears `DefaultConfig` caches.
- Errors: user `Encode`/`Decode` errors surface on `Marshal`/`Unmarshal`; decoding truncated input with custom types.
- Coexistence: `TypeConverters` still applied on `any` paths with custom types registered.
- Schema resolution: reader/writer schemas with promoted types and custom logical type on the reader side.
- Allocation and race tests: `AllocsPerRun` zero for typed scalar form; `-race` with concurrent `Marshal` while codecs build.
- Benchmarks as listed under Performance.
