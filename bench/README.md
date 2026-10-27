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

Other flags: `-data` (full datasets, see below), `-rows`, `-java` (JMH JSON, see
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
| `earthquakes` | real | 1 707 | USGS weekly earthquake feed (Feb 2018, full snapshot), nullable numbers, timestamps, string arrays |
| `wikirc` | synthetic | 2 500 | Wikimedia recent-change events (mediawiki/recentchange schema): enum, uuid, timestamp-millis, nullable records, map and array |
| `ghwebhooks` | real | 329 | GitHub webhook event payloads (octokit examples), deeply nested, many nullable records, URLs, maps, timestamps |

Samples are committed as zstd-compressed JSON Lines (`sample.jsonl.zst`,
best compression, no Git LFS) and decompressed once in memory, outside timing.
Licences and provenance live in each dataset's `NOTICE`.

Full datasets are optional and never committed:

```sh
go run ./cmd/fetch                 # downloads pinned URLs into data/<dataset>/, verifies SHA-256
go run ./cmd/report -data=data     # benchmarks the full files instead of the samples
go run ./cmd/report -data=data -rows=200000   # caps every dataset at 200 000 records to bound memory
```

## Formats

| Format | Library | Ops |
|---|---|---|
| `avro` (baseline) | this library, reflection | all |
| `hamba` | github.com/hamba/avro/v2 | all |
| `json` | encoding/json | single, batch (NDJSON) |
| `protobuf` | google.golang.org/protobuf, generated messages | single, batch (`protodelim`) |

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
