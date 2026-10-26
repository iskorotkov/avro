# Design: Avro JSON encoding

Status: proposal (design only). Tracks hamba/avro#209 from `ISSUES.md`.

## Problem

The library speaks only Avro binary. Users who need JSON, for debugging, REST gateways, `avro-tools tojson/fromjson` interop, or Kafka Connect JSON sinks, today `Unmarshal` into `any` and run `encoding/json` over the result. That path is wrong in several ways:

- Record field order is lost because records decode to `map[string]any` (the literal ask in hamba/avro#209).
- Unions come out as `{"string": "x"}` only by accident of the generic map representation, and named branches use whatever key the generic decoder chose.
- `bytes`/`fixed` become base64 (from `encoding/json`), not the ISO-8859-1 strings the spec requires.
- `NaN`/`Inf` make `encoding/json` fail, and large `long` values survive only because Go keeps them as `int64` (any round-trip through `float64` breaks them).
- There is no way back: JSON produced by Java's `JsonEncoder` cannot be read at all.

Since the json-iterator removal (`0483048`), the module uses stdlib `encoding/json` only, and only for schema/protocol parsing and the registry/OCF metadata. Nothing in the data path touches JSON.

## Goals and non-goals

Goals:

- Spec-compliant Avro JSON encoding (1.12.0, "JSON Encoding") for every schema type, interoperable with Java `JsonEncoder`/`JsonDecoder` and `avro-tools`.
- Schema-order record output, so #209 is solved without any ordered-map type.
- Binary-to-JSON and JSON-to-binary transcoding without Go types, plus typed `v any` encode/decode reusing existing struct tags, `TypeResolver`, `TypeConverter` and `UnionConverter`.
- Opt-in and additive: no change to existing signatures or behaviour, zero cost on the binary path.

Non-goals:

- "Pretty" logical types in JSON (RFC 3339 timestamps, decimal as a number); the spec says logical types use their underlying encoding.
- Schema resolution inside the JSON decoder; we decode JSON with the writer schema and resolve on the binary side (see Architecture).
- Replacing `encoding/json` for schema parsing.

## Spec summary of the tricky parts

| Avro type | JSON form | Notes |
|---|---|---|
| null / boolean / string | `null`, `true`, string | Strings must be valid UTF-8; invalid bytes are replaced by U+FFFD (Java does the same). |
| int / long | integer literal | Decode `long` straight to `int64`, never via `float64`; reject non-integral numbers such as `1.5`. |
| float / double | number | Spec is silent on NaN/Inf. Java (Jackson) writes `"NaN"`, `"Infinity"`, `"-Infinity"` as quoted strings; goavro/linkedin writes `null` and `±1e999`; fastavro writes bare `NaN`. |
| bytes / fixed | string | Each byte becomes the code point U+0000..U+00FF (ISO-8859-1). On decode, a code point above U+00FF is an error; fixed length must equal `size`. |
| enum | symbol string | Unknown symbol is an error (enum `default` applies only during resolution). |
| array / map | JSON array / object | Map keys are strings; we preserve wire order when transcoding. |
| record | object, fields in schema order | Missing field: use schema default if present, else error; unknown field: error unless `AllowUnknownFields`. |
| union | `null` for the null branch, otherwise `{"<branch name>": value}` | Branch name is the primitive name or the named type's **full name**. This "tagged" form is the spec; many Go/JS tools emit the "bare" value instead (AVRO-2899). |
| logical types | underlying type | `decimal` bytes are the two's-complement unscaled value as an ISO-8859-1 string; `uuid` is a string; `duration` is fixed(12); timestamps are `long`. |

Defaults: field default values in the schema use the same JSON mapping **except** that a union default is the bare value of the first branch (no tag). Defaults are parsed by `encoding/json` into `any`, so `long` defaults above 2^53 already lose precision; see Open questions.

## Comparison with twmb/avro

twmb ships `Schema.EncodeJSON / AppendEncodeJSON / DecodeJSON` (`json_codec.go`, `json_decode.go`, `json_scan.go`, ~3.7k lines) with options `TaggedUnions`, `TagLogicalTypes`, `LinkedinFloats`, `SkipUnknown`, `AliasInput`. Lessons:

- **Bare unions by default, tagged opt-in.** Their docs admit Java, fastavro and `avro-tools fromjson` reject the default output. We invert this: tagged by default (spec), bare opt-in.
- **Liberal decoder, strict encoder.** The decoder accepts tagged and bare unions, all three NaN/Inf conventions, `long.timestamp-millis` branch names, and the bare `-Infinity` token (disambiguated with `peekAt`). We adopt this; the cost is tiny and it makes cross-tool data readable.
- **Bare-union ambiguity.** A bare `7` matches int/long/float/double and a bare `"x"` matches string/bytes/fixed/enum; twmb picks the first branch in declaration order, which can silently pick the wrong branch and skip branch-bound custom types. We do the same for `any` targets but, for typed targets, prefer the branch whose compiled codec matches the Go type.
- **NaN in bare unions does not round-trip** under the linkedin convention (`null` is claimed by the null branch). Another reason to default to tagged + quoted strings.
- **Bytes output escapes non-ASCII as `\u00XX`**, keeping the output pure ASCII and byte-exact; decoding accepts both escaped and raw UTF-8 forms of U+0080..U+00FF.
- **Hand-written scanner and appenders** (`jsonScanner`, `appendJSONString`) instead of `encoding/json`; zero-allocation token reads. We follow suit.
- twmb walks a schema tree per call with reflection; it has no per-type compiled codec cache. Ours does, which shapes the architecture below.
- Resolved JSON decode in twmb decodes JSON then resolves through a binary decode. We use the same layering deliberately.

## Proposed API

A new subpackage `github.com/iskorotkov/avro/v2/avrojson`, built only on the root package's exported API.

```go
package avrojson

// UnionMode selects how non-null union values are written.
type UnionMode uint8

const (
	// UnionTagged writes {"type": value}, as the spec and Java require.
	UnionTagged UnionMode = iota
	// UnionBare writes the branch value without a wrapper.
	UnionBare
)

// FloatMode selects how NaN and infinities are written.
type FloatMode uint8

const (
	// FloatJava writes the strings "NaN", "Infinity" and "-Infinity".
	FloatJava FloatMode = iota
	// FloatLinkedIn writes null for NaN and ±1e999 for infinities.
	FloatLinkedIn
	// FloatError fails the encode on any non-finite value.
	FloatError
)

// Config is the JSON codec configuration; the zero value is spec-compliant.
type Config struct {
	API                avro.API  // Binary API used for typed values; nil means avro.DefaultConfig.
	Unions             UnionMode // Union output form; decoding always accepts both forms.
	Floats             FloatMode // Non-finite float output; decoding accepts all conventions.
	AllowUnknownFields bool      // Ignore JSON object keys that are not record fields.
	Indent             string    // Non-empty enables multi-line output with this indent.
}

// Freeze validates the config and returns a reusable, goroutine-safe codec.
func (c Config) Freeze() *Codec

// Transcoding without Go types; this is the direct answer to hamba/avro#209.
func (c *Codec) BinaryToJSON(dst []byte, schema avro.Schema, bin []byte) ([]byte, error)
func (c *Codec) JSONToBinary(dst []byte, schema avro.Schema, js []byte) ([]byte, error)

// Typed values, using the same Go mapping as avro.Marshal and avro.Unmarshal.
func (c *Codec) Marshal(schema avro.Schema, v any) ([]byte, error)
func (c *Codec) AppendMarshal(dst []byte, schema avro.Schema, v any) ([]byte, error)
func (c *Codec) Unmarshal(schema avro.Schema, data []byte, v any) error

// Streams of newline-delimited values, matching avro-tools tojson output.
func (c *Codec) NewEncoder(schema avro.Schema, w io.Writer) *Encoder
func (c *Codec) NewDecoder(schema avro.Schema, r io.Reader) *Decoder

// Package-level helpers use a default spec-compliant Codec.
func Marshal(schema avro.Schema, v any) ([]byte, error)
func Unmarshal(schema avro.Schema, data []byte, v any) error
```

Naming: we deliberately avoid `MarshalJSON`/`UnmarshalJSON` as method names, because `go vet`'s `stdmethods` check flags them when the signature differs from `json.Marshaler`, and readers would assume stdlib semantics. We also do not add methods to `avro.API`: that interface is implementable by users, and every addition is a breaking change (the recent `AppendMarshal` addition already paid that cost once). An `avro.API`-level shortcut can be added later if demand appears.

## Architecture

Three options were weighed:

1. **Separate typed JSON codec tree** (mirror of `codec_*.go` with `JSONEncode(ptr, *jsonWriter)`), cached per (schema, type) next to the binary caches. Fastest typed path, but duplicates ~6k lines of Go-type mapping (struct tags, embedded structs, converters, `UnionConverter`, resolver, `DisallowUnknownFields`), and every future binary codec feature must be ported twice.
2. **Generic `any` path**: `Unmarshal` into `any`, then walk. Simple, but loses order (records become maps), allocates heavily, and still needs schema walking to emit tags and bytes.
3. **Schema-compiled transcoder plus binary intermediate (proposed).** For each schema, compile a tree of small transcoder nodes (`binToJSON(r *avro.Reader, out []byte) []byte` and `jsonToBin(s *scanner, w *avro.Writer)`), cached by `schema.CacheFingerprint()` in a `sync.Map` owned by the `Codec`. Typed `Marshal` is `api.AppendMarshal` into a pooled buffer, then `BinaryToJSON`; typed `Unmarshal` is `JSONToBinary` into a pooled buffer, then `api.Unmarshal`.

Why option 3:

- All Go-type semantics come from the existing compiled binary codecs, so JSON and binary can never disagree on how a struct maps to a schema.
- The transcoder depends only on the schema, so the cache key is the fingerprint alone and the tree is small (one node per schema node, recursive refs via a deferred node like `deferDecoder`).
- Field order, map order and union branch identity are read straight from the wire, which is exactly what #209 needs.
- Schema resolution comes for free: decode JSON with the writer schema, then `avro.Unmarshal` with a resolved reader schema as today.
- It needs only exported API (`avro.Reader`/`avro.Writer` read/write primitives, `RecordSchema.Fields()`, `UnionSchema.Types()`, `NamedSchema.FullName()`, logical schemas), which is what makes the subpackage possible.

Why a subpackage rather than `json_*.go` files in the root:

- It is a hard guarantee that the binary package's code size, init work and hot paths do not change, and that no `Config` field or `API` method is added.
- Users who never need JSON never link it.
- The cost is that a later option-1 fast path would need internal hooks; if that happens it can live in the root behind an internal package without changing the `avrojson` API.

JSON I/O uses a hand-written appender and a zero-allocation scanner modelled on twmb's `json_scan.go` (strconv for numbers, `strconv.AppendFloat(..., 'g', -1, 32|64)` for floats). `encoding/json` stays confined to schema parsing; its reflection and float64 numbers are unsuitable for the data path. `encoding/json/v2`'s `jsontext` is attractive but should not be adopted until it is out of `GOEXPERIMENT` in the module's minimum Go version.

Decoding rules for unions: `null` selects the null branch; a single-key object whose key is a branch name (full name, short name when unambiguous, or `type.logical`) selects that branch; anything else is tried as a bare value against branches by JSON token class in declaration order. A union containing a record or map branch makes single-key objects genuinely ambiguous in bare mode; tagged interpretation wins, and this is documented.

## Performance

- Binary path: untouched, no new branches, no new fields in `frozenConfig`; verified by running the existing `bench_*_test.go` suite with `benchstat` before and after (expect no diff since no root-package file changes).
- Transcoder: target zero allocations per call for `BinaryToJSON` into a reused `dst`, and at most one per call for `JSONToBinary` (pooled `avro.Writer`).
- Typed path pays one extra binary encode/decode. Binary encoding is typically several times cheaper than JSON formatting (varints vs. decimal/escape work), so the expected overhead is 15-30%. If benchmarks show more, option 1 can be added later behind the same API.
- New benchmarks: `BenchmarkBinaryToJSON`, `BenchmarkJSONToBinary`, typed Marshal/Unmarshal on the `superhero` schema, compared against `json.Marshal` of the equivalent struct and twmb's `EncodeJSON`/`DecodeJSON` (alongside the existing `bench_twmb_apis_test.go`).

## Compatibility

- Purely additive: a new package, no change to root-package exported identifiers or behaviour.
- Output defaults to the spec (tagged unions, Java float strings), so it round-trips with Java and `avro-tools`. Users wanting goavro/JS-style output opt into `UnionBare`/`FloatLinkedIn`.
- Generic `any` targets get the same union representation `avro.Unmarshal` already produces (`map[string]any{"branch": v}` when unresolved), because typed decode goes through the binary codecs.

## Alternatives considered

- **Option 1 / option 2** above: rejected for duplication and for losing order, respectively.
- **Adding `Config.JSON` options and `API.MarshalJSON`** in the root package: breaks `API` implementers, trips `go vet`, and grows the binary package.
- **An ordered map type for `any` decoding** (alternative fix for #209): changes the generic representation for everyone and still does not give spec JSON.
- **Wrapping `encoding/json` with custom `Marshaler`s on generated types**: only works for generated code, cannot emit union tags without schema context.

## Open questions

1. Should field defaults be re-parsed with `json.Decoder.UseNumber` so `long` defaults above 2^53 are exact? This touches schema parsing and is arguably a separate bug.
2. Should missing record fields without a default be an error (Java `JsonDecoder`) or zero-valued? Proposal: error, with no option until someone asks.
3. Branch-name matching for named types: accept the short name only if unique in the union, or always require the full name as Java does?
4. Should `Indent` exist at all, or should users pipe through `json.Indent`?
5. Is a typed fast path (option 1) worth it for `Unmarshal` from JSON, where the extra binary hop is most visible?

## Test plan

- **Spec table tests** per type: every row of the table above, both directions, including empty strings/bytes, `fixed` length mismatch, code points > U+00FF, `-0.0`, `math.MaxInt64`, `math.MinInt64`, and every NaN/Inf convention on decode.
- **Union matrix**: tagged and bare input for `["null", T]` for every T, multi-branch unions with token-class collisions, named branches by full and short name, `long.timestamp-millis` names, and NaN inside unions under each `FloatMode`.
- **Round-trip property tests**: random schema plus random binary datum (reuse `fuzz_decoder_test.go` generators), check `JSONToBinary(BinaryToJSON(b)) == b` byte-for-byte.
- **Fuzzing**: `FuzzJSONToBinary` and `FuzzBinaryToJSON` must never panic and must respect `MaxByteSliceSize`-style limits.
- **Java interop fixtures** under `avrojson/testdata/java/`: a small generator (documented, run manually with `avro-tools` 1.12.0) writing `.avsc`, `.avro` (binary datum) and `.json` (`avro-tools tojson`) triplets covering every type, nested unions, decimals and NaN/Inf. Tests assert our `BinaryToJSON` equals Java's JSON semantically (parsed and compared), and that our output is accepted by `avro-tools fromjson` (checked in as expected binary).
- **twmb parity**: decode twmb's `TaggedUnions` output and bare output for the same data.
- **Typed path**: run the existing encoder/decoder struct fixtures through `avrojson.Marshal`/`Unmarshal` and compare with `avro.Marshal` + `BinaryToJSON`, including `TypeConverter`, `UnionConverter` and `DisallowUnknownFields` cases.
- **Regression for #209**: a record with 20 fields decoded from binary emits keys in schema order.
- **Binary benchmarks** unchanged under `benchstat`.
