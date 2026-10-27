# Benchmark results

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Processor @ 2.80GHz |
| OS/arch | linux/amd64 |
| Go | go1.26.7 |
| Commit | 2d099e13073a3f1327e3a77526257f9dd84aec6a |
| Date | 2026-10-27T20:09:48Z |
| GOMAXPROCS | 4 |
| Flags | `-count=6 -benchtime=100ms -procs=4 -dataset=.* -op=.* -format=.*` |

How to read: percentages are relative to the `avro` baseline (100%, lower is better); ± is the 95% CI half-width; ⚠ marks high variance or too few samples.

![Summary](results/charts/summary.svg)

## nyctaxi

### encode

![nyctaxi encode](results/charts/nyctaxi-encode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 5.778ms ±10% ⚠ | 100% | 3.467M ±10.0% ⚠ | 381.1MB/s ±10.0% ⚠ | 2.125MiB ±0.0% | 100% | 20.03k | 100% | 109.9B | 100% |
| hamba | 6.726ms ±12% ⚠ | 116% | 2.979M ±13% ⚠ | 327.4MB/s ±13% ⚠ | 2.125MiB ±0.0% | 100% | 20.03k | 100% | 109.9B | 100% |
| protobuf | 8.453ms ±33% ⚠ | 146% | 2.374M ±24% ⚠ | 267.2MB/s ±24% ⚠ | 2.265MiB | 107% | 20.03k | 100% | 112.6B | 102% |
| json | 40.67ms ±29% ⚠ | 704% | 492.5k ±19% ⚠ | 200.2MB/s ±19% ⚠ | 9.797MiB ±0.0% | 461% | 60.10k ±0.0% | 300% | 406.4B | 370% |

### decode

![nyctaxi decode](results/charts/nyctaxi-decode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 10.81ms ±22% ⚠ | 100% | 1.853M ±18% ⚠ | 203.7MB/s ±18% ⚠ | 4.254MiB ±0.0% | 100% | 115.5k | 100% | 109.9B | 100% |
| protobuf | 11.21ms ±5.9% ⚠ | 104% | 1.788M ±5.8% ⚠ | 201.3MB/s ±5.8% ⚠ | 4.542MiB ±0.0% | 107% | 115.5k | 100% | 112.6B | 102% |
| hamba | 18.30ms ±9.3% ⚠ | 169% | 1.095M ±8.5% ⚠ | 120.4MB/s ±8.5% ⚠ | 4.254MiB ±0.0% | 100% | 115.5k | 100% | 109.9B | 100% |
| json | 128.3ms ±5.9% ⚠ | 1187% | 156.1k ±5.9% ⚠ | 63.44MB/s ±5.9% ⚠ | 8.362MiB ±0.0% | 197% | 195.6k | 169% | 406.4B | 370% |

### batch-encode

![nyctaxi batch-encode](results/charts/nyctaxi-batch-encode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 4.763ms ±35% ⚠ | 100% | 4.206M ±23% ⚠ | 462.2MB/s ±23% ⚠ | 632.0B | 100% | 3.000 | 100% | 109.9B | 100% |
| hamba | 4.743ms ±4.2% | 100% | 4.224M ±4.0% | 464.2MB/s ±4.0% | 600.0B | 95% | 3.000 | 100% | 109.9B | 100% |
| protobuf | 10.88ms ±10% ⚠ | 228% | 1.843M ±11% ⚠ | 209.4MB/s ±11% ⚠ | 2.418MiB ±0.0% | 401215% | 40.06k | 1335500% | 113.6B | 103% |
| json | 45.36ms ±18% ⚠ | 952% | 442.5k ±18% ⚠ | 180.3MB/s ±18% ⚠ | 1.834MiB ±0.0% | 304303% | 40.06k ±0.0% | 1335500% | 407.4B | 371% |

### batch-decode

![nyctaxi batch-decode](results/charts/nyctaxi-batch-decode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 11.04ms ±34% ⚠ | 100% | 1.829M ±26% ⚠ | 201.0MB/s ±26% ⚠ | 4.568MiB ±0.0% | 100% | 115.5k | 100% | 109.9B | 100% |
| protobuf | 13.36ms ±28% ⚠ | 121% | 1.500M ±21% ⚠ | 170.4MB/s ±21% ⚠ | 4.858MiB ±0.0% | 106% | 115.5k | 100% | 113.6B | 103% |
| hamba | 22.10ms ±28% ⚠ | 200% | 922.6k ±27% ⚠ | 101.4MB/s ±27% ⚠ | 4.568MiB ±0.0% | 100% | 115.5k | 100% | 109.9B | 100% |
| json | 124.3ms ±27% ⚠ | 1126% | 161.2k ±20% ⚠ | 65.69MB/s ±20% ⚠ | 4.551MiB | 100% | 115.5k | 100% | 407.4B | 371% |

### container-read-deflate

![nyctaxi container-read-deflate](results/charts/nyctaxi-container-read-deflate.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 28.08ms ±4.7% | 100% | 713.4k ±4.4% | 22.61MB/s ±4.4% | 18.41MiB ±0.0% | 100% | 128.2k ±0.0% | 100% | 31.70B | 100% |
| hamba | 39.05ms ±25% ⚠ | 139% | 513.0k ±19% ⚠ | 16.26MB/s ±19% ⚠ | 18.41MiB ±0.0% | 100% | 128.3k ±0.0% | 100% | 31.70B | 100% |

### container-read-null

![nyctaxi container-read-null](results/charts/nyctaxi-container-read-null.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 12.42ms ±7.8% ⚠ | 100% | 1.614M ±7.8% ⚠ | 177.8MB/s ±7.8% ⚠ | 7.019MiB ±0.0% | 100% | 117.9k ±0.0% | 100% | 110.2B | 100% |
| hamba | 19.16ms ±24% ⚠ | 154% | 1.045M ±18% ⚠ | 115.2MB/s ±18% ⚠ | 7.020MiB ±0.0% | 100% | 118.0k ±0.0% | 100% | 110.2B | 100% |

### container-read-snappy

![nyctaxi container-read-snappy](results/charts/nyctaxi-container-read-snappy.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 14.25ms ±11% ⚠ | 100% | 1.406M ±11% ⚠ | 63.84MB/s ±11% ⚠ | 7.949MiB ±0.0% | 100% | 118.1k ±0.0% | 100% | 45.39B | 100% |
| hamba | 22.08ms ±19% ⚠ | 155% | 907.2k ±16% ⚠ | 41.19MB/s ±16% ⚠ | 7.949MiB ±0.0% | 100% | 118.2k ±0.0% | 100% | 45.39B | 100% |

### container-read-zstd

![nyctaxi container-read-zstd](results/charts/nyctaxi-container-read-zstd.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 16.11ms ±11% ⚠ | 100% | 1.244M ±11% ⚠ | 43.36MB/s ±11% ⚠ | 8.098MiB ±0.0% | 100% | 118.2k ±0.0% | 100% | 34.85B | 100% |
| hamba | 22.48ms ±30% ⚠ | 140% | 891.2k ±20% ⚠ | 31.05MB/s ±20% ⚠ | 8.099MiB ±0.1% | 100% | 118.3k ±0.0% | 100% | 34.85B | 100% |

### container-write-deflate

![nyctaxi container-write-deflate](results/charts/nyctaxi-container-write-deflate.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 125.3ms ±8.5% ⚠ | 100% | 159.9k ±8.5% ⚠ | 5.070MB/s ±8.5% ⚠ | 158.4MiB ±0.0% | 100% | 3.720k ±0.0% | 100% | 31.70B | 100% |
| hamba | 126.0ms ±10% ⚠ | 101% | 158.9k ±10% ⚠ | 5.040MB/s ±10% ⚠ | 158.4MiB | 100% | 3.720k | 100% | 31.70B | 100% |

### container-write-null

![nyctaxi container-write-null](results/charts/nyctaxi-container-write-null.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 5.563ms ±27% ⚠ | 100% | 3.635M ±26% ⚠ | 400.5MB/s ±26% ⚠ | 59.90KiB ±0.0% | 100% | 100.0 | 100% | 110.2B | 100% |
| hamba | 4.901ms ±51% ⚠ | 88% | 4.088M ±28% ⚠ | 450.4MB/s ±28% ⚠ | 59.82KiB | 100% | 100.0 | 100% | 110.2B | 100% |

### container-write-snappy

![nyctaxi container-write-snappy](results/charts/nyctaxi-container-write-snappy.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 9.938ms ±13% ⚠ | 100% | 2.016M ±11% ⚠ | 91.50MB/s ±11% ⚠ | 2.620MiB | 100% | 302.0 | 100% | 45.39B | 100% |
| hamba | 10.50ms ±4.9% | 106% | 1.907M ±5.1% ⚠ | 86.57MB/s ±5.1% ⚠ | 2.619MiB | 100% | 301.0 | 100% | 45.39B | 100% |

### container-write-zstd

![nyctaxi container-write-zstd](results/charts/nyctaxi-container-write-zstd.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 39.24ms ±18% ⚠ | 100% | 510.6k ±21% ⚠ | 17.80MB/s ±21% ⚠ | 8.702MiB | 100% | 484.0 | 100% | 34.85B | 100% |
| hamba | 38.17ms ±15% ⚠ | 97% | 524.9k ±11% ⚠ | 18.29MB/s ±11% ⚠ | 10.92MiB | 126% | 511.0 | 106% | 34.85B | 100% |

## superhero

### encode

![superhero encode](results/charts/superhero-encode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 514.9µs ±17% ⚠ | 100% | 1.942M ±14% ⚠ | 324.1MB/s ±14% ⚠ | 171.6KiB ±0.0% | 100% | 1.000k | 100% | 166.9B | 100% |
| hamba | 567.9µs ±11% ⚠ | 110% | 1.767M ±12% ⚠ | 294.9MB/s ±12% ⚠ | 171.6KiB | 100% | 1.000k | 100% | 166.9B | 100% |
| protobuf | 906.8µs ±9.7% ⚠ | 176% | 1.103M ±9.2% ⚠ | 220.2MB/s ±9.2% ⚠ | 199.7KiB | 116% | 1.000k | 100% | 199.6B | 120% |
| json | 2.987ms ±26% ⚠ | 580% | 334.8k ±20% ⚠ | 189.7MB/s ±20% ⚠ | 586.2KiB ±0.0% | 342% | 1.000k | 100% | 566.6B | 339% |

### decode

![superhero decode](results/charts/superhero-decode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 914.9µs ±9.6% ⚠ | 100% | 1.093M ±8.6% ⚠ | 182.5MB/s ±8.6% ⚠ | 443.4KiB ±0.0% | 100% | 7.440k | 100% | 166.9B | 100% |
| hamba | 1.006ms ±36% ⚠ | 110% | 994.0k ±28% ⚠ | 165.9MB/s ±28% ⚠ | 443.4KiB ±0.0% | 100% | 7.440k | 100% | 166.9B | 100% |
| protobuf | 1.737ms ±13% ⚠ | 190% | 576.8k ±13% ⚠ | 115.2MB/s ±13% ⚠ | 715.1KiB ±0.0% | 161% | 16.14k | 217% | 199.6B | 120% |
| json | 11.70ms ±27% ⚠ | 1279% | 85.47k ±21% ⚠ | 48.42MB/s ±21% ⚠ | 805.5KiB ±0.0% | 182% | 23.14k | 311% | 566.6B | 339% |

### batch-encode

![superhero batch-encode](results/charts/superhero-batch-encode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 427.1µs ±32% ⚠ | 100% | 2.343M ±23% ⚠ | 390.9MB/s ±23% ⚠ | 632.0B | 100% | 3.000 | 100% | 166.9B | 100% |
| hamba | 462.9µs ±29% ⚠ | 108% | 2.161M ±23% ⚠ | 360.6MB/s ±23% ⚠ | 600.0B | 95% | 3.000 | 100% | 166.9B | 100% |
| protobuf | 1.032ms ±14% ⚠ | 242% | 969.3k ±12% ⚠ | 195.2MB/s ±12% ⚠ | 207.5KiB | 33620% | 2.001k | 66700% | 201.3B | 121% |
| json | 3.002ms ±12% ⚠ | 703% | 334.3k ±13% ⚠ | 189.7MB/s ±13% ⚠ | 96.00B | 15% | 1.000 | 33% | 567.6B | 340% |

### batch-decode

![superhero batch-decode](results/charts/superhero-batch-decode.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 871.8µs ±10% ⚠ | 100% | 1.148M ±11% ⚠ | 191.6MB/s ±11% ⚠ | 461.0KiB | 100% | 7.614k | 100% | 166.9B | 100% |
| hamba | 1.141ms ±21% ⚠ | 131% | 876.3k ±22% ⚠ | 146.2MB/s ±22% ⚠ | 461.0KiB | 100% | 7.614k | 100% | 166.9B | 100% |
| protobuf | 1.749ms ±17% ⚠ | 201% | 572.0k ±15% ⚠ | 115.2MB/s ±15% ⚠ | 735.2KiB ±0.0% | 159% | 16.14k | 212% | 201.3B | 121% |
| json | 11.37ms ±12% ⚠ | 1304% | 87.98k ±12% ⚠ | 49.94MB/s ±12% ⚠ | 535.0KiB | 116% | 16.15k | 212% | 567.6B | 340% |

### container-read-deflate

![superhero container-read-deflate](results/charts/superhero-container-read-deflate.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 2.919ms ±13% ⚠ | 100% | 342.6k ±13% ⚠ | 29.86MB/s ±13% ⚠ | 1.397MiB ±0.0% | 100% | 9.429k ±0.0% | 100% | 87.14B | 100% |
| hamba | 2.966ms ±16% ⚠ | 102% | 337.8k ±14% ⚠ | 29.43MB/s ±14% ⚠ | 1.398MiB ±0.0% | 100% | 9.463k | 100% | 87.14B | 100% |

### container-read-null

![superhero container-read-null](results/charts/superhero-container-read-null.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 1.153ms ±17% ⚠ | 100% | 867.7k ±17% ⚠ | 145.5MB/s ±17% ⚠ | 713.1KiB ±0.0% | 100% | 8.987k ±0.0% | 100% | 167.6B | 100% |
| hamba | 1.119ms ±23% ⚠ | 97% | 893.5k ±17% ⚠ | 149.8MB/s ±17% ⚠ | 713.7KiB ±0.0% | 100% | 9.020k ±0.0% | 100% | 167.6B | 100% |

### container-read-snappy

![superhero container-read-snappy](results/charts/superhero-container-read-snappy.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 1.143ms ±22% ⚠ | 100% | 875.1k ±16% ⚠ | 95.69MB/s ±16% ⚠ | 825.6KiB ±0.0% | 100% | 8.998k | 100% | 109.3B | 100% |
| hamba | 1.186ms ±18% ⚠ | 104% | 843.4k ±14% ⚠ | 92.23MB/s ±14% ⚠ | 826.2KiB ±0.0% | 100% | 9.031k ±0.0% | 100% | 109.3B | 100% |

### container-read-zstd

![superhero container-read-zstd](results/charts/superhero-container-read-zstd.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 1.626ms ±17% ⚠ | 100% | 620.5k ±16% ⚠ | 53.95MB/s ±16% ⚠ | 1.091MiB ±0.0% | 100% | 9.080k ±0.0% | 100% | 86.93B | 100% |
| hamba | 1.613ms ±9.2% ⚠ | 99% | 620.6k ±8.8% ⚠ | 53.95MB/s ±8.9% ⚠ | 1.092MiB ±0.0% | 100% | 9.113k | 100% | 86.93B | 100% |

### container-write-deflate

![superhero container-write-deflate](results/charts/superhero-container-write-deflate.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 11.42ms ±6.6% ⚠ | 100% | 87.62k ±6.7% ⚠ | 7.635MB/s ±6.7% ⚠ | 8.008MiB | 100% | 234.0 | 100% | 87.14B | 100% |
| hamba | 10.98ms ±24% ⚠ | 96% | 91.06k ±18% ⚠ | 7.935MB/s ±18% ⚠ | 8.008MiB ±0.0% | 100% | 234.0 | 100% | 87.14B | 100% |

### container-write-null

![superhero container-write-null](results/charts/superhero-container-write-null.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 462.0µs ±8.0% ⚠ | 100% | 2.166M ±7.9% ⚠ | 363.1MB/s ±7.9% ⚠ | 88.14KiB | 100% | 53.00 | 100% | 167.6B | 100% |
| hamba | 518.1µs ±10.0% ⚠ | 112% | 1.931M ±11% ⚠ | 323.8MB/s ±11% ⚠ | 88.06KiB | 100% | 53.00 | 100% | 167.6B | 100% |

### container-write-snappy

![superhero container-write-snappy](results/charts/superhero-container-write-snappy.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 799.5µs ±23% ⚠ | 100% | 1.252M ±20% ⚠ | 136.9MB/s ±20% ⚠ | 291.3KiB | 100% | 65.00 | 100% | 109.3B | 100% |
| hamba | 863.7µs ±16% ⚠ | 108% | 1.165M ±16% ⚠ | 127.4MB/s ±16% ⚠ | 291.2KiB | 100% | 64.00 | 98% | 109.3B | 100% |

### container-write-zstd

![superhero container-write-zstd](results/charts/superhero-container-write-zstd.svg)

| Format | time/op | vs base | records/s | MB/s | B/op | vs base | allocs/op | vs base | wire B/rec | vs base |
|---|---|---|---|---|---|---|---|---|---|---|
| **avro** | 7.543ms ±26% ⚠ | 100% | 132.9k ±24% ⚠ | 11.55MB/s ±24% ⚠ | 6.591MiB | 100% | 241.0 | 100% | 86.93B | 100% |
| hamba | 7.354ms ±16% ⚠ | 98% | 136.0k ±19% ⚠ | 11.82MB/s ±19% ⚠ | 8.811MiB | 134% | 268.0 | 111% | 86.93B | 100% |

