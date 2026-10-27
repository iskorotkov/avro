# Snappy reference fixtures

Raw Snappy blocks for `ocf/codec_snappy_test.go`. They were produced once, so the tests need no network access and no non-Go tools.

| File | Content |
|------|---------|
| `record.bin` | 8 KiB of concatenated Avro-encoded `superhero` records (`testdata/superhero.avsc`) with varied fields |
| `low.bin` | 8 KiB of random bytes from `abcd` (low entropy) |
| `const.bin` | 8 KiB of `a` |
| `*.snappy` | the matching `*.bin` compressed by the C++ reference libsnappy 1.1.10 (`snappy_compress` from `snappy-c.h`), the codec behind snappy-java and Avro Java |
| `s2-repeat.s2` | a block from `github.com/klauspost/compress/s2.Encode` that uses the S2 repeat-offset copy; libsnappy 1.1.10 `snappy_validate_compressed_buffer` rejects it |

To regenerate a `.snappy` file, link a small C program against `libsnappy.so.1` that calls `snappy_compress` on the `.bin` file and writes the result.
