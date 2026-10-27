# Java

Apache Avro Java 1.12 benchmarked with [JMH](https://github.com/openjdk/jmh) on
the same `schema.avsc` and `sample.jsonl.zst` files as the Go suite, so records
and wire bytes match.

Needs JDK 21 and Maven. Everything installs into `target/` and `~/.m2`.

```sh
./run.sh                                  # full run, writes target/report.json (~15 min)
./run.sh -o /tmp/java.json -f 1 -wi 1 -i 1 -w 200ms -r 200ms -p dataset=superhero   # quick smoke
cd .. && go run ./cmd/report -java java/target/report.json
cd .. && go run ./cmd/report -java-run -java-args='-f 1 -i 3'   # run Java first when java and mvn exist
```

`run.sh` builds `target/benchmarks.jar`, runs it with `-prof gc`, and converts
the JMH JSON to the ingestion format. Extra arguments go to JMH; defaults
(2 forks, 3 warmup and 5 measured 1 s iterations) live on `SuiteBenchmark`.

## Layout

```
pom.xml                           # avro, avro-maven-plugin (generates SpecificRecord classes), jmh, zstd-jni
src/main/java/io/github/iskorotkov/avrobench/
  dataset/                        # Dataset enum, zstd JSONL sample loading, JSON to GenericRecord
  subject/                        # Subject (records + datum reader/writer) for generic and specific APIs
  workload/                       # the ops, each with a round-trip check
  jmh/                            # SuiteBenchmark (@Param dataset/format/op) and Convert (adds sizes)
```

- Formats: `java-avro-generic` (`GenericRecord`, strings decode to `Utf8`) and
  `java-avro-specific` (generated classes, `String`, `Instant` timestamps).
- Container files flush a block every 100 records, like the Go encoder.
- `encode` reuses one buffer and encoder (idiomatic Java) while Go allocates a
  result per record, so Java gets a small advantage there.
- Ops use the Go names: `encode`, `decode`, `batch-encode`, `batch-decode`,
  `container-write-<codec>`, `container-read-<codec>` for `null`, `deflate`
  (level 6), `snappy` and `zstd` (level 3), each over the whole sample.
- Every workload is verified (decoded equals original) in `@Setup` before
  timing. Sample decompression is outside timing.
- `Convert` runs the same workloads once to add the `records` and `wireBytes`
  params, since JMH cannot set params from `@Setup`.

## Ingestion format

The report reads JMH JSON after `Convert`.

Rules (implemented and documented in `report/jmh`):

- mode must be `avgt`; `scoreUnit` one of `ns/op`, `us/op`, `ms/op`, `s/op`;
- `params.dataset` and `params.op` are required and use the Go vocabulary;
  `params.format` is optional and defaults to `java-avro`;
- each `primaryMetric.rawData` value (all forks) is one sample, named
  `BenchmarkSuite/dataset=<d>/op=<op>/format=<f>`;
- one JMH op processes the whole sample, like one Go iteration; the report
  normalises time per record, so per-record ops still compare correctly;
- optional params `records` (records per op) and `wireBytes` (encoded bytes per
  op) add per-record time, `rec/s`, MB/s and `wire-B/rec`;
- secondary metric `gc.alloc.rate.norm` (from `-prof gc`) becomes B/op.

Example: `report/jmh/testdata/example.json`.
