## Breaking changes

- Decoder limits can no longer be disabled. `Config.MaxByteSliceSize`, `Config.MaxSliceAllocSize`, `Config.MaxMapAllocSize`, `ocf.WithMaxBlockBytes` and `ocf.WithMaxDecompressedBlockBytes` treat zero and negative values, including `-1`, as "use the default". Previously `-1` disabled `MaxByteSliceSize`, and zero or negative values left the other limits unbounded. To accept larger data, set an explicit large value. Values above the platform allocation limit are clamped to it.
- Stricter defaults: `Config.MaxSliceAllocSize` and `Config.MaxMapAllocSize` default to 67,108,864 items in total per decoded value, instead of the platform allocation limit. `ocf.WithMaxBlockBytes` and `ocf.WithMaxDecompressedBlockBytes` default to 64 MiB instead of unlimited. `Config.MaxByteSliceSize` stays at 1 MiB and `Config.MaxDepth` stays at 1000.
- Every numeric size or limit option treats zero or a negative value as its documented default. `ocf.WithBlockLength(0)` used to write unbounded blocks, and a negative value flushed every record into its own block; both now select 100 records. A negative `ocf.WithBlockSize` used to flush every record; it now selects the default, which bounds blocks by record count only.

- The default OCF encoder bounds blocks by record count only, so 100 large records can produce a block above the decoder's 64 MiB default. Readers of such files must raise `ocf.WithMaxBlockBytes` and `ocf.WithMaxDecompressedBlockBytes`, and writers can keep blocks small with `ocf.WithBlockSize`.

## Security

- Untrusted input is bounded without any configuration: forged array and map counts fail at 67,108,864 items per value, bytes and strings at 1 MiB, and OCF blocks at 64 MiB compressed and decompressed, instead of exhausting memory.
- OCF decoders cap decompressed blocks at 64 MiB by default for every built-in codec (deflate, snappy, zstandard); zero or a negative `ocf.WithMaxDecompressedBlockBytes` selects that default. Snappy checks the decoded length in the block header before allocating, so an 11-byte block claiming 4 GiB is rejected without allocating it.
