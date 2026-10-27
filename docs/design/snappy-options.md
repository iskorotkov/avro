# OCF Snappy codec: implementation options

Status: decision support, 2026-10-27. Scope: `ocf.SnappyCodec` (Avro OCF block = raw Snappy block + big-endian CRC32 of the uncompressed bytes).

## Options

| ID | Encoder | Decoder | Notes |
|----|---------|---------|-------|
| A | `golang/snappy.Encode` | `golang/snappy.Decode` | current `main` |
| B | `s2.EncodeSnappy` | `klauspost/compress/snappy.DecodeStrict` | branch `build/snappy-s2` |
| C | `s2.EncodeSnappy` | `golang/snappy.Decode` | keeps both libraries |
| D | `s2.EncodeSnappy` | Snappy-only pre-scan + `s2.Decode` | ~50 lines of owned tag-walking code |
| E1 | `s2.EncodeSnappyBetter` | `DecodeStrict` | same as `klauspost/compress/snappy.Encode` |
| E2 | `s2.EncodeSnappyBest` | `DecodeStrict` | |
| F | — | — | no other maintained independent pure-Go Snappy block codec was found: `eapache/go-xerial-snappy` is a framing wrapper over a Snappy package, and `minio/minlz` uses an incompatible format |

The only S2 extension that is valid in a raw S2 block but invalid in Snappy is the repeat-offset copy (copy-1 tag with offset 0). The D pre-scan rejects exactly that tag and leaves all other validation to `s2.Decode`.

## Method

- Prototypes: one build tag per option over a shared `snappyEncode`/`snappyDecode` seam in `ocf/codec.go`. They were kept in a local worktree and not pushed.
- Data: blocks of 16 KiB, 64 KiB and 1 MiB of four kinds:
  - `record`: concatenated Avro-encoded `superhero` records with varied fields;
  - `low`: 4-symbol random text;
  - `const`: a single repeated byte;
  - `random`: random bytes.
- Benchmarks: `-benchtime=200ms -count=6`, two lanes with `GOMAXPROCS=2` each, compared with benchstat on a 2.8 GHz Xeon (amd64). Run-to-run noise is about ±15%: B, C and D share an encoder yet differ by up to 15%.
- Interop:
  - Go output decoded by the C++ reference libsnappy 1.1.10, which is the native codec that snappy-java (used by Avro Java) wraps, and by `golang/snappy`.
  - Reference output decoded by every option.
  - Running the Avro Java jar itself was not possible in the sandbox. Avro Java's framing is `Snappy.compress` plus a big-endian CRC32 of the uncompressed data, the same as ours.
- Robustness:
  - every truncation of a 16 KiB block;
  - 2,000 random bit flips;
  - a CRC-mismatch block;
  - a 1 GiB-claim header ("memory bomb");
  - 60 s of differential fuzzing of `SnappyCodec.Decode` against `golang/snappy` (accept/reject and output must match) for B and D.

## Results

Throughput is the geomean over all 12 data/size cases, relative to A. "~" means within noise.

| Axis | A | B | C | D | E1 | E2 |
|------|---|---|---|---|----|----|
| Encode throughput | 1.00 | ~1.02 | ~1.05 | ~1.10 | 0.67 | 0.09 |
| Encode, `record` 16–64 KiB | 1.00 | +18% | +31–46% | +29–48% | −33–50% | −93% |
| Encode, `low` 1 MiB | 1.00 | −23% | −20% | −16% | −39% | −94% |
| Encode B/op | 1.25× input | 1.13× (−10–14%) | same as B | same as B | ≈B | noisy |
| Decode throughput | 1.00 | **0.70** | ~0.97 | 0.87 | 0.68 | 0.65 |
| Decode, `const`/`low` 16–64 KiB | 1.00 | −37–60% | ~ | −26–35% | −46–58% | −50–64% |
| Decode allocs | 1 × output | same | same | same | same | same |
| Size, `record` (% of input) | 61.5–63.5 | +0.5–1.2% (worse) | =B | =B | −1.0–1.4% | −1.9–2.2% |
| Size, `low` | 50.2–50.8 | −6% to +11% | =B | =B | −10–16% | −16–21% |
| Size, `const` | 4.7 | +6.6% (worse) | =B | =B | +6.6% | +7% |
| Rejects S2-only block | yes | yes | yes | yes (pre-scan) | yes | yes |
| C++ ref ⇄ Go, all data | pass | pass | pass | pass | pass | pass |
| Truncation, bit flips, CRC | pass | pass | pass | pass | pass | pass |
| Differential fuzz (60 s) | — | pass | ≡A | pass | ≡B | ≡B |
| 11-byte bomb, uncapped | 1 GiB alloc | 1 GiB | 1 GiB | 1 GiB | 1 GiB | 1 GiB |
| Bomb with `WithMaxDecompressedBlockBytes` | rejected | rejected | rejected | rejected | rejected | rejected |
| Direct deps | +golang/snappy | −1 | +golang/snappy | −1 | −1 | −1 |
| Binary delta (stripped) | 0 | +24.6 KB | +24.6 KB | +28.7 KB | +20.5 KB | +12.3 KB |
| Owned codec code | none | none | none | ~50 lines parser | none | none |

### Why the B and E decoders are slow

`DecodeStrict` is a pure-Go byte loop. `golang/snappy` and `s2.Decode` both have amd64/arm64 assembly.

### Libraries

- `golang/snappy`:
  - Last release v1.0.0 (2023-12-25); not archived; low activity.
  - No advisories found.
  - Has amd64/arm64 assembly and a `noasm` tag.
- `klauspost/compress`:
  - Very active, with a single maintainer; latest release v1.20.3 (2026-10-12).
  - `snappy.DecodeStrict` exists only since v1.19.1, so B pins a young API.
  - GHSA-259r-337f-4rfw (S2 dictionaries, fixed in v1.18.7) does not touch block Snappy.
  - GHSA-fpmh-frq8-7rj8 (zstd decoder panic, fixed in v1.20.2) **does affect this repo's pinned v1.19.2 zstd codec**, whichever Snappy option is chosen.

### User-facing compatibility

- All options produce valid Snappy, so Java, C++ and `golang/snappy` readers keep working.
- Under B, C, D and E, byte-for-byte output differs from A. Golden-size tests (such as the old `938` assertion) change, but readers do not.
- No public API changes.

## Recommendation

**Keep A (main) and do not merge `build/snappy-s2` as is.** B's main gain is one fewer `go.mod` line. It costs about 30% decode throughput, up to 60% on compressible blocks. OCF files are usually read more often than written. B also gives no ratio gain on realistic record blocks (slightly worse) and depends on a decoder API that is only weeks old.

- If removing `golang/snappy` is a requirement, prefer **D**. It recovers most of the decode speed (−13%), matched `golang/snappy` in differential fuzzing, and keeps B's encoder. Its cost is a small owned parser that needs its own fuzz test.
- Consider E1 only if file size matters more than encode speed.
- C has no reason to exist: it keeps both dependencies and has a worse ratio.

Independent follow-ups:

1. Bump `klauspost/compress` to ≥ v1.20.2 (GHSA-fpmh-frq8-7rj8, zstd).
2. Consider a default cap on Snappy decoded length. Today an 11-byte block allocates up to 4 GiB unless `WithMaxDecompressedBlockBytes` is set, in every option.
3. Port the interop, S2-rejection and differential fuzz tests from the prototype to `main`; they are useful regardless of the option chosen.
4. Revisit B if `DecodeStrict` gains an assembly fast path.
