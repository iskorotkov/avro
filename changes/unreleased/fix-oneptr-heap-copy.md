## Security

- Encoding a pointer-shaped value no longer hands user methods (such as a pointer-receiver `MarshalText`) the address of a temporary stack copy, which they could retain and later read as garbage. The value is copied to the heap whenever it, or a struct field or array element stored inline in it, has pointer-receiver methods. This covers top-level `Marshal` and `Encoder.Encode`, record field defaults on encode and decode, and map-encoded unions.

## Behaviour changes

- Marshalling a pointer-shaped value whose type (or an inline field or element type) has pointer-receiver methods now makes one extra pointer-sized allocation per call.
