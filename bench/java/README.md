# Java (planned)

Apache Avro Java will be benchmarked with [JMH](https://github.com/openjdk/jmh)
in a follow-up. Nothing here is built yet.

## Planned layout

```
java/
  pom.xml                       # org.apache.avro:avro, avro-maven-plugin, jmh-core
  src/main/java/.../Datasets.java  # reads ../datasets/<name>/sample.jsonl.zst (zstd-jni + Jackson)
  src/main/java/.../AvroBench.java # @State per dataset, @Param dataset/op
```

- Uses the same `schema.avsc` and samples as the Go suite, so records and wire
  bytes match; generated `SpecificRecord` classes and `GenericRecord` variants.
- Same operations and per-op unit of work as Go: `encode`, `decode`,
  `batch-encode`, `batch-decode`, `container-write-<codec>`,
  `container-read-<codec>`, each over the whole sample.
- Round trip verified in `@Setup` before measuring.

## Ingestion format

The report reads native JMH JSON:

```sh
java -jar target/benchmarks.jar -bm avgt -tu ns -rf json -rff results.json -prof gc
cd .. && go run ./cmd/report -java java/results.json
```

Rules (implemented and documented in `report/jmh`):

- mode must be `avgt`; `scoreUnit` one of `ns/op`, `us/op`, `ms/op`, `s/op`;
- `params.dataset` and `params.op` are required and use the Go vocabulary;
  `params.format` is optional and defaults to `java-avro`;
- each `primaryMetric.rawData` value (all forks) is one sample, named
  `BenchmarkSuite/dataset=<d>/op=<op>/format=<f>`;
- one JMH op processes the whole sample, like one Go iteration; the report
  normalises time per record, so per-record ops still compare correctly;
- optional params `records` (records per op) and `wireBytes` (encoded bytes per
  op), set as constants in `@Setup`, add per-record time, `rec/s`, MB/s and
  `wire-B/rec`;
- secondary metric `gc.alloc.rate.norm` (from `-prof gc`) becomes B/op.

Example: `report/jmh/testdata/example.json`.
