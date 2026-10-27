# Benchmark suite

Compares serialisation libraries on synthetic and real-life datasets. This is a
separate Go module (`github.com/iskorotkov/avro/bench`), so comparison
dependencies never reach the library's `go.mod` and the root `go test ./...`
does not include it.

Latest results: [RESULTS.md](RESULTS.md).

## Running the report

```sh
cd bench
go run ./cmd/report                                   # full run: 6 rounds, 200ms per benchmark
go run ./cmd/report -count=3 -benchtime=50ms          # quicker
go run ./cmd/report -dataset=nyctaxi -op='^(en|de)code$' -format='avro|hamba'
go run ./cmd/report -from results/latest.txt          # re-render without running
go run ./cmd/report -cpuprofile=results/profiles      # one CPU profile per round
```

The tool compiles the suite once, then runs it `-count` times as separate
processes with `GOMAXPROCS=-procs`, rotating the format order every round so
machine drift spreads across formats. It then:

- saves raw `go test -bench` output to `results/<date>-<sha>.txt` and
  `results/latest.txt` (only `latest.txt` is committed);
- computes medians and 95% confidence intervals with `golang.org/x/perf`
  (`benchmath`) and flags results whose CI half-width exceeds `-noise`;
- normalises time, B/op, allocs/op and encoded size to 100% = `-baseline`
  (`avro`, this library via reflection) per dataset and op, next to absolute
  values ± spread;
- writes [RESULTS.md](RESULTS.md) and SVG charts to `results/charts/`
  (light and dark mode aware, embeddable in READMEs).

Other flags: `-data` (full datasets, see below), `-java` (JMH JSON, see
[java/README.md](java/README.md)), `-out`, `-charts`, `-results`, `-procs`.
Run `go run ./cmd/report -h` for the full list.

Close other applications and keep the machine on AC power; numbers from
shared CI runners are too noisy for comparisons, so CI only checks that every
benchmark builds and round-trips (`-benchtime=1x`).

## Operations and metrics

Every operation processes the whole dataset per iteration. Each adapter's
round trip is verified (decoded value equals the original) before timing, so
a broken adapter fails instead of benchmarking an error path.

| Op | Meaning |
|---|---|
| `encode` / `decode` | one `Marshal`/`Unmarshal` call per record |
| `batch-encode` / `batch-decode` | all records through the format's streaming encoder/decoder |
| `container-write-<codec>` / `container-read-<codec>` | a container file (Avro OCF) with `null`, `deflate`, `snappy` or `zstd` blocks |

Metrics: `ns/op`, `MB/s` (encoded bytes), `B/op`, `allocs/op`, plus `ns/rec`,
`rec/s` and `wire-B/rec` (encoded bytes per record).

## Datasets

| Dataset | Kind | Records | Notes |
|---|---|---|---|
| `superhero` | synthetic | 1 000 | nested records and arrays, schema from `testdata/superhero.avsc` |
| `nyctaxi` | real | 20 032 | NYC TLC yellow taxi trips (Jan 2024), flat, numeric, nullable fields, timestamps |

Samples are committed as zstd-compressed JSON Lines (`sample.jsonl.zst`,
best compression, no Git LFS) and decompressed once in memory, outside timing.
Licences and provenance live in each dataset's `NOTICE`.

Full datasets are optional and never committed:

```sh
go run ./cmd/fetch                 # downloads pinned URLs into data/<dataset>/, verifies SHA-256
go run ./cmd/report -data=data     # benchmarks the full files instead of the samples
```

## Formats

| Format | Library | Ops |
|---|---|---|
| `avro` (baseline) | this library, reflection | all |
| `hamba` | github.com/hamba/avro/v2 | all |
| `json` | encoding/json (v1; reimplemented on v2 when built with `-goexperiment=jsonv2`) | single, batch (NDJSON) |
| `json-exp` | github.com/go-json-experiment/json | single, batch |
| `json-v2` | encoding/json/v2, only with `-goexperiment=jsonv2` | single, batch |
| `goccy` | github.com/goccy/go-json | single, batch |
| `sonic` | github.com/bytedance/sonic, amd64/arm64 only (skipped elsewhere) | single, batch |
| `protobuf` | google.golang.org/protobuf, generated messages | single, batch (`protodelim`) |
| `msgp` | github.com/tinylib/msgp, generated methods | single, batch |
| `msgpack` | github.com/vmihailenco/msgpack/v5, reflection | single, batch |
| `cbor` | github.com/fxamacker/cbor/v2, reflection | single, batch |

Build-tagged formats need the report tool to pass the toolchain settings; the
flags are recorded in the `RESULTS.md` header:

```sh
go run ./cmd/report -goexperiment=jsonv2    # adds json-v2 and runs json (v1) on the v2 implementation
go run ./cmd/report -tags=sometag           # any other build tags
```

Run once without and once with `-goexperiment=jsonv2` to compare `json` on v1
and on v2; runs with different tags or experiments write separate result files
(`results/<date>-<sha>-jsonv2.txt`), but `latest.txt` and `RESULTS.md` hold the last run.

Struct-based formats (JSON, MessagePack, CBOR) use the `json` tags of the
canonical records and encode structs as maps keyed by field name, so sizes are
comparable. Timestamps are exact in every format: JSON and `cbor` use RFC 3339
text (`cbor`'s epoch modes lose sub-second precision, so it is larger and
slower than its integer mode would be), `msgpack` the standard MessagePack
timestamp extension and `msgp` its own 15-byte time extension (its standard
one, `//msgp:newtime`, breaks the streaming reader in v1.6.5). Equality is `cmp.Equal`, which compares `time.Time`
with `Equal`, so the location a decoder attaches does not matter. `msgp` encodes
mirror types generated into `formats/msgp` (`go generate`).

## Extending

**Add a dataset**: create `datasets/<name>` with a type implementing
`dataset.Provider` (`Name`, `Sources`, `Load`), a canonical record type with
`avro` and `json` tags, `schema.avsc`, `sample.jsonl.zst` (write it with
`dataset.WriteJSONL`, usually from a `-update` test) and a `NOTICE` with the
licence; then add it to `datasets.All`. Reflection-based formats pick it up
automatically; code-generated formats skip it until they add a binding.

**Add a format**: create `formats/<name>` with a type implementing
`format.Format` (`Name`, `Bind`), returning a `format.Subject` with a
`format.Codec` and optional `format.BatchCodec` / `format.ContainerCodec`
(`format.StreamBatch` and `format.StreamContainer` wrap streaming APIs); return
`format.ErrUnsupported` for datasets it cannot bind. Then add it to
`formats.All`. Code-generated formats keep per-dataset schemas and converters
in their own package (see `formats/protobuf`, regenerated with `go generate`).
