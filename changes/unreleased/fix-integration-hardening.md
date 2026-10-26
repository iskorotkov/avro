## Security

- Registry client: subjects are escaped as single URL path segments; empty, `.`, `..` and control-character subjects fail with `registry.ErrInvalidPathSegment` before any request is sent.
- Registry client: response bodies, including error bodies, are capped (`WithMaxResponseBytes`).
- `avrogen` refuses to generate code for names that are not valid Go identifiers (`gen.ErrInvalidIdentifier`) or cannot be placed in a struct tag (`gen.ErrUnsafeTagValue`), even with `avro.SkipNameValidation = true`; enum values are emitted as escaped string literals; line breaks in doc comments cannot escape the comment.
- `resolvers.MemorySchemaStore.AddSchema` returns `resolvers.ErrFingerprintCollision` instead of overwriting a different schema with the same fingerprint.
- CI no longer persists checkout credentials.

## Behaviour changes

Registry client options (all optional, set through `registry.NewClient`):

| Option | Default | 0 or negative | User-visible effect |
|---|---|---|---|
| `WithMaxResponseBytes(n)` | 8 MiB | no cap | A response of exactly `n` bytes is accepted, `n+1` fails with an error naming the limit. Large `GetSubjects` responses may hit the cap; an oversized error body yields an `Error` with the status code and no message. |
| `WithSchemaMissTTL(d)` | 5s | not remembered | A not-found or unparsable schema ID is remembered for `d`, so a schema registered by another process is not visible through `GetSchema` for up to that long. `CreateSchema` and `IsRegistered` on the same client clear the memory. |
| `WithSchemaCacheSize(n)` | unbounded | unbounded | Past `n` cached schemas an arbitrary entry is evicted and fetched again on next use. |

## Added

- `registry.WithMaxResponseBytes`, `registry.WithSchemaMissTTL`, `registry.WithSchemaCacheSize`, `registry.DefaultMaxResponseBytes`, `registry.DefaultSchemaMissTTL`.
- `gen.ErrInvalidIdentifier`, `gen.ErrUnsafeTagValue`, `resolvers.ErrFingerprintCollision`, `registry.ErrInvalidPathSegment`.
