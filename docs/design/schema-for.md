# Design: `SchemaFor[T]` — derive an Avro schema from a Go type

Status: proposal (design only, no code yet)
Scope: package `github.com/iskorotkov/avro/v2`, new files `schema_for.go` and `schema_for_test.go`

## Problem

Today a user of this library writes the schema as JSON and then writes a matching Go struct
with `avro:"name"` tags (or generates the struct from the schema with `avrogen`). There is no
way to go the other direction: start from an existing Go type and get a `Schema`. Teams that
treat Go types as the source of truth (event producers, registry auto-registration, tests)
keep hand-written JSON in sync with structs and only find drift at runtime, when
`Marshal` fails with a field mismatch.

## Goals

- `SchemaFor[T]()` returns a parsed `Schema` that `Marshal`/`Unmarshal` accept for `T` with no extra config.
- Reuse the existing `avro` tag (name before the first comma) and the README "Types Conversions" table, inverted.
- Tag options for schema-only metadata: field defaults, aliases, logical types, inline, skip.
- Deterministic output, so the schema fingerprint is stable across runs and builds.
- Interop with `avrogen`: a generated struct's schema is used verbatim when available.
- Purely additive: no change to how existing codecs read tags or resolve schemas.

## Non-goals

- Inferring schemas for `any`, `map[string]any`, or interface-typed fields (no static shape).
- General unions beyond `["null", T]`; users supply those via an override hook.
- Schema evolution or compatibility decisions (use `SchemaCompatibility`).
- Changing codec behaviour for new tag options (for example, `omitzero` does not affect encoding).
- Doc strings from Go comments (needs source parsing; out of scope for a reflect-based API).

## Comparison with twmb/avro

twmb/avro ships `SchemaFor[T any](opts ...SchemaOpt) (*Schema, error)` (`schema_for.go`, ~1.3k lines,
~6.4k lines of tests). What we adopt and where we differ:

| Aspect | twmb/avro | This proposal |
| --- | --- | --- |
| Entry point | `SchemaFor[T]`, `MustSchemaFor[T]` | Same names, return `Schema` (interface) |
| Build strategy | Build a JSON tree, then `Parse` it | Same: emit JSON tree, then `ParseBytes` via a private `SchemaCache` |
| Pointers | `*T` -> `["null", T]`, default `null`, pointer chains collapse | Same; chains rejected beyond what `codec_ptr.go` supports |
| `time.Time` | `long`/`timestamp-millis`, overridable by tag | Default `timestamp-micros` (see open question) |
| `time.Duration` | `int`/`time-millis`, override `time-micros` only | Same |
| `*big.Rat` | requires `decimal(p,s)` tag | Same; `bytes` by default, `fixed` when `fixed=N` given |
| uuid | `,uuid` on `string`, TextMarshaler, or `[16]byte` | `,uuid` on `string` only (codec support is string-only today) |
| Tag options | `-`, `inline`, `omitzero`, `alias=`, `type-alias=`, `default=` (last), logical names, `decimal(p,s)` | Same set minus `omitzero` effects; adds `enum=[...]`, `fixed=N` |
| Recursion | `seen` map keyed by `reflect.Type`, emits name reference | Same |
| Name conflicts | dedups identical definitions, errors on conflicting ones | Same, error names both Go types |
| Custom types | `CustomType` registry option | Hook via `SchemaProvider` interface + `WithTypeSchema` option |

Lessons from twmb's test suite worth copying: reject unknown tag options instead of ignoring them,
reject logical tags on the wrong Go kind, bound recursion depth for non-struct cycles
(`type T []T`), keep tag-split rules identical between schema inference and the codec,
and verify every generated schema by actually encoding and decoding a value.

## Proposed API

```go
// SchemaFor returns the Avro schema for the Go type T using the default config.
func SchemaFor[T any](opts ...SchemaForOption) (Schema, error)

// MustSchemaFor is like SchemaFor but panics on error.
func MustSchemaFor[T any](opts ...SchemaForOption) Schema

// SchemaForType is the non-generic form for callers that only have a reflect.Type.
func SchemaForType(typ reflect.Type, opts ...SchemaForOption) (Schema, error)

// SchemaFor on API honours TagKey and the cache of the given config.
func (c *frozenConfig) SchemaFor(typ reflect.Type, opts ...SchemaForOption) (Schema, error)

type SchemaForOption func(*schemaForConfig)

// WithNamespace sets the namespace of the top-level record and of nested named types.
func WithNamespace(ns string) SchemaForOption

// WithName overrides the top-level record name, which defaults to the Go type name.
func WithName(name string) SchemaForOption

// WithNameFunc maps a nested Go type to an Avro name, defaulting to reflect.Type.Name().
func WithNameFunc(fn func(reflect.Type) string) SchemaForOption

// WithTypeSchema pins the schema used for every occurrence of a Go type.
func WithTypeSchema(typ reflect.Type, s Schema) SchemaForOption

// WithTimeLogical sets the default logical type used for time.Time fields.
func WithTimeLogical(lt LogicalType) SchemaForOption

// SchemaProvider lets a type supply its own schema, which generated structs already satisfy.
type SchemaProvider interface{ Schema() Schema }
```

`T` may be a struct, a pointer to a struct, or any other supported type (a top-level `[]Event`
yields an array schema). `API.SchemaFor` is added to the `API` interface only if we accept a
minor-version interface extension; otherwise it lives on `frozenConfig` and is reached through
a package helper `SchemaForConfig(api API, typ reflect.Type, ...)` (decided in review).

## Type mapping (Go -> Avro)

This is the README table inverted, picking one canonical Avro type per Go type.

| Go type | Avro schema | Notes |
| --- | --- | --- |
| `bool` | `boolean` | |
| `int8`, `int16`, `int32`, `uint8`, `uint16` | `int` | |
| `int`, `int64`, `uint32` | `long` | `int` is `long` to be safe on 64-bit |
| `uint`, `uint64`, `uintptr` | error | no lossless Avro type; use `[8]byte` / `fixed` via tag |
| `float32` / `float64` | `float` / `double` | |
| `string` | `string` | `,uuid` -> `string`+`uuid`; `,enum=[A,B]` -> `enum` |
| `[]byte` | `bytes` | |
| `[N]byte` | `fixed` size N | name: Go type name, else `fixed_N` |
| `[]T`, `[N]T` (T != byte) | `array` of T | |
| `map[string]T` | `map` of T | other key kinds are an error |
| `struct` | `record` | recursive, see naming |
| `*T` | `["null", T]`, default `null` | `**T` collapses; `*[]T`, `*map` allowed |
| `time.Time` | `long`+`timestamp-micros` | tags: `date`, `timestamp-*`, `local-timestamp-*` |
| `time.Duration` | `int`+`time-millis` | tag `time-micros` -> `long` |
| `*big.Rat`, `big.Rat` | `bytes`+`decimal(p,s)` | `decimal(p,s)` tag required; with `fixed=N` -> `fixed` |
| `avro.LogicalDuration` | `fixed(12)`+`duration` | recognised by type, takes no tag |
| type with `TextMarshaler`+`TextUnmarshaler` | `string` | matches `codec_marshaler.go`; `,enum=` allowed |
| type implementing `SchemaProvider` | its schema | covers avrogen output |
| `any`, interfaces, `chan`, `func`, `complex*` | error | override with `WithTypeSchema` |

Enums: Go has no closed enum, so `enum` is only produced from an explicit `enum=[A,B,C]` tag on a
string-kind or TextMarshaler field, or from `SchemaProvider`. The enum name is the Go type name
(named string types) or `<Record><Field>Enum` for plain `string` fields.

## Tag grammar

```
tag      = name [ "," option { "," option } ]
name     = "" | "-" | avro-name
option   = "inline" | "omitzero" | logical | "alias=" list | "type-alias=" list
         | "enum=" list | "fixed=" int | "decimal(" int "," int ")" | "default=" rest
logical  = "uuid" | "date" | "time-millis" | "time-micros" | "timestamp-millis"
         | "timestamp-micros" | "timestamp-nanos" | "local-timestamp-millis"
         | "local-timestamp-micros" | "local-timestamp-nanos"
list     = avro-name | "[" avro-name { "," avro-name } "]"
rest     = any text to end of tag, parsed as JSON, else taken as a JSON string
```

- Splitting respects `(...)` and `[...]`, and `default=` must be last because it consumes the rest.
- `avro:"-"` skips the field; `-,anything` is an error (twmb's exact-match rule).
- An empty name keeps the Go field name, which is what `describeStruct` already does.
- Unknown options are an error in `SchemaFor` but stay ignored by codecs, as today.
- `default=` is validated against the field schema by `Parse`; for nullable fields the default
  reorders the union so the default branch comes first, as the spec requires.

**Codec invariant.** `describeStruct` in `codec_record.go` calls `strings.Cut(tag, ",")` and keeps
only the name, so every option above is invisible to encoding and decoding. `SchemaFor` must derive
field names with the same rule (shared helper), so a field name never disagrees between the two.

## Naming, recursion and duplicates

- Top-level name: `WithName`, else `NameFunc(T)`; anonymous struct types require `WithName`.
- Nested named types (records, enums, fixed) get `NameFunc(t)` in the `WithNamespace` namespace;
  anonymous nested structs get `<Parent><Field>` in PascalCase.
- Names are validated against the Avro name grammar; generic instantiations (`Page[int]`) are
  sanitised to `Page_int` by the default name func.
- Embedded structs are flattened exactly like `describeStruct` (Go promotion rules, shallower wins,
  ambiguous names error); `,inline` flattens a named, non-embedded struct field the same way.
- A `seen map[reflect.Type]string` is filled before fields are visited, so self- and mutual
  recursion (`type Node struct { Next *Node }`) emits a name reference. Recursion that does not pass
  through a pointer, slice or map cannot be built in Go, so the nullable/array wrapper always exists.
- Non-struct recursive types (`type T []T`) are rejected with a depth bound.
- Two different Go types mapping to the same full name: identical definitions are merged,
  differing ones fail with an error naming both Go types and the Avro name.
- Field order follows Go declaration order; output is deterministic.

## Round-trip guarantee

For every `T` that `SchemaFor[T]` accepts and every value `v` of `T`:

1. `Unmarshal(s, MustMarshal(s, v), &out)` succeeds and `out` equals `v` modulo documented
   precision loss (time truncated to the logical unit, `nil` vs empty slices and maps).
2. `Parse(s.String())` yields a schema with the same `Fingerprint()` (the schema is canonical-stable).
3. `avrogen` on `s` produces a struct whose fields have the same `avro` names and Go types as `T`
   for the canonical mappings in the table (not for `uint8`/`int16` and similar, which widen).
4. `SchemaFor` on an avrogen struct returns the generated schema unchanged via `SchemaProvider`.

The test suite enforces (1)-(4); any mapping that cannot satisfy (1) is an error, not a schema.

## Compatibility

- New files only; no exported identifiers change. `SchemaFor` is a new name in the package.
- Existing struct tags keep working: the name part is read identically, and options are ignored by
  codecs. Structs that already use tag options for other reasons are unaffected at runtime;
  `SchemaFor` may reject them, which is a new call and not a regression.
- `Config.TagKey` is honoured through the config-bound variant.
- No new dependencies; build uses only `reflect`, `encoding/json` and the existing parser.

## Alternatives considered

- **Build `Schema` objects directly** with `NewRecordSchema` etc. instead of JSON: avoids a parse,
  but default validation, union rules and name resolution would be duplicated. JSON-then-Parse keeps
  one source of truth; the cost is paid once per type and can be cached by callers.
- **Separate tag key (`avroschema:"..."`)**: zero ambiguity with codecs, but duplicates the field
  name in two tags and invites drift, which is the problem being solved.
- **Code generation (Go -> JSON) via `go generate`**: no reflection at runtime, but needs a new tool
  and cannot see types from other modules' reflection-only info; can be layered on later by
  calling `SchemaFor` from a tiny generator.
- **Adopt twmb's API wholesale (`SchemaOpt` shared with `Parse`)**: our `Parse` has no options type,
  so a dedicated `SchemaForOption` is simpler.

## Open questions

1. Default `time.Time` logical: twmb uses `timestamp-millis`; `timestamp-micros` loses less data
   and matches Java/Spark defaults. Proposal: micros, configurable via `WithTimeLogical`.
2. Should `int` map to `long` unconditionally, or follow the platform word size?
3. Should `uint64` map to `fixed(8)` since the codec supports it (`fixedUint64Codec`), or stay an error?
4. Should `omitzero` mean anything (for example, implying a nullable union)? Proposal: reserved, no effect.
5. Doc strings: accept a `doc=` option, or leave docs to `WithTypeSchema`?
6. Should the result be cached per `(type, options)` inside the config, like codec caches?

## Test plan

- Table tests per row of the type mapping, asserting exact canonical JSON output.
- Tag parser tests: every option, bracket and parenthesis nesting, `default=` with commas,
  unknown options, `-,x` rejection, empty names, invalid Avro names.
- Shared-helper test asserting `describeStruct` and `SchemaFor` produce the same field-name set for
  embedded, shadowed and ambiguous struct layouts.
- Recursion tests: self, mutual, through slice and map, and `type T []T` rejection.
- Duplicate-name tests: same type reused (merged), two types with one name (error).
- Round-trip property tests: random values of fixture structs through `Marshal`/`Unmarshal`.
- avrogen tests: run `gen` on `SchemaFor` output for fixtures and compare field names and types;
  and feed generated structs back into `SchemaFor` to check the `SchemaProvider` path.
- Fingerprint stability test: build the same schema repeatedly and across `GOARCH=386`.
- Fuzz test on tag strings to make sure the tag parser never panics.
