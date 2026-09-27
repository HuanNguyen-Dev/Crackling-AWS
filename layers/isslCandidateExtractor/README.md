# Layer: `isslCandidateExtractor`

This layer contains the precompiled Linux x86_64 candidate Extractor used by
the Crackling AWS candidate-extraction pipeline. Each invocation hydrates one
compressed ISSL bucket for one contiguous, half-open global off-target ID
interval. The Extractor combines bucket IDs with signature bytes, compressed
occurrence bytes, and the applicable overflow rows downloaded by the Lambda
wrapper.

The native source is `isslExtractCandidates.cpp` in the Crackling scoring
source tree. Build it in the Lambda-compatible Python 3.10 Amazon Linux 2
container from the directory containing that source:

```bash
  sudo docker run --rm \
      --platform linux/amd64 \
      --entrypoint /bin/bash \
      -v "$PWD:/src" \
      -w /src \
      public.ecr.aws/lambda/python:3.10 \
      -lc '
        yum install -y gcc-c++ &&
        g++ -o extractor \
          isslExtractCandidates.cpp \
          -O3 \
          -std=c++11 \
          -static-libgcc \
          -static-libstdc++ &&
        chmod 755 extractor
      '
```

Copy the resulting binary to `layers/isslCandidateExtractor/extractor` in the
AWS repository. Verify its dynamic dependencies in the same Lambda container:

```bash
ldd /extractor
```

The current Extractor resolves only Amazon Linux core libraries (`libm` and
`libc`), so it does not require additional shared libraries in `layers/lib`.

## Compressed input and hydration contract

The ISSL global catalogue layout is, in order:

```text
uint64 N
uint64 sequenceLength
uint64 sliceCount
N * 5 signature bytes
N * 1 occurrence bytes
uint64 overflowCount
overflowCount * 16 overflow rows
masks and compressed bucket lists
```

An occurrence byte in the range 1–254 is the complete count. Sentinel `255`
means the full `uint32` count is stored in a sorted 16-byte overflow row:

```text
uint64 globalId
uint32 occurrences
4 padding bytes
```

The Lambda wrapper downloads only the signature and occurrence ranges for its
ID partition. It binary-searches the sorted overflow table by global ID and
downloads only rows in that partition. The native Extractor receives:

```text
extractor signatures.bin occurrences.bin overflow.bin bucket.bin \
  elementCount startId endId idBits totalCount candidates.bin
```

It validates and resolves sentinel occurrences while it writes hydrated
candidates. Mapper never receives the raw occurrence bytes or overflow table.

Hydrated output is headerless, little-endian, and packed:

| Global ID width | Record format | Bytes |
| --- | --- | --- |
| 32-bit | `<QII>`: signature, ID, occurrences | 16 |
| 64-bit | `<QQI>`: signature, ID, occurrences | 20 |

The candidate Mapper consumes only these hydrated records during off-target
scoring.

## Working-set and output-size bound

The Lambda downloads a catalogue partition, maps the current compressed bucket,
and truncates `candidates.bin` before processing each bucket. A bucket is a
compressed LEB128 global-ID list. Every bucket entry whose ID is in the
Extractor's half-open interval can produce at most one hydrated record;
occurrences remain a field and never expand into repeated records.

For capacity planning, let:

- `P = ceil(offtargetsCount / extractorCount)`, the largest ID partition;
- `K`, the complete overflow-row count;
- `B`, the current compressed bucket size in bytes;
- `E`, the bucket's decoded entry count; and
- `R`, the hydrated record width (16 or 20 bytes).

The raw catalogue download is bounded by:

```text
6 * P + 16 * min(K, P)
```

For one bucket, the conservative working-set estimate is:

```text
6 * P + 16 * min(K, P) + B + min(E, P) * R + safetyMargin
```

The selected bucket and output paths are reused sequentially. The Lambda logs
actual catalogue, bucket, hydrated-output, and combined local byte counts so
the estimate can be compared with production behaviour.

The dynamic allocator therefore distinguishes two cases:

- `RETRY_WITH_MORE_EXTRACTORS`: the bucket fits, but the catalogue partition
  or estimated output requires a larger `N`.
- `BUCKET_EXCEEDS_LAMBDA_LIMIT`: the compressed bucket and its maximum hydrated
  output already exceed the configured safe working limit, so increasing the
  partition count cannot make the task safe.

The Dispatcher can calculate this before publishing extraction work because
`shards.json` contains `offtargetsCount`, the overflow count, and every
bucket's byte boundaries and decoded-entry counts. This model primarily
protects Lambda `/tmp`; memory-map residency and native process overhead still
require a safety margin against the Lambda memory limit.
