# Layer: `isslCandidateExtractor`

This layer contains the precompiled Linux x86_64 candidate Extractor used by
the Crackling AWS candidate-extraction pipeline. Each invocation hydrates one
raw ISSL bucket for one contiguous global off-target ID interval by combining
the bucket's IDs and occurrence counts with the corresponding signatures from
a catalogue partition.

The source implementation used for this binary is
`isslExtractCandidates.cpp`. Its build command is:

```text
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
        chmod 755 extractor &&
        file extractor &&
        ldd extractor
      '
```

The `isslCandidateExtractor` layer was introduced into this AWS repository in
commit `8b4e66d` (`Feature: Added candidate extraction lambda pipeline`). The
precompiled `extractor` binary was produced outside this repository from the
supplied local
`isslExtractCandidates.cpp`; that source directory is:
https://github.com/HuanNguyen-Dev/Crackling/commit/65e5b69736d3f88ee17a5d63dfdeb0f797b38496,
in the commit `65e5b69736d3f88ee17a5d63dfdeb0f797b38496`.

The Extractor writes headerless 16-byte little-endian records matching
`<QII`: a `uint64` signature, `uint32` global off-target ID, and `uint32`
occurrence count. The candidate Mapper consumes these hydrated records during
off-target scoring.

## Working-set and output-size bound

The native implementation memory-maps its catalogue partition and the current
raw bucket, and truncates `candidates.bin` before processing each bucket. A raw
bucket contains 8-byte packed records (`uint32` occurrence count and `uint32`
global ID). Every raw record whose ID belongs to the extractor's half-open ID
range can produce at most one 16-byte hydrated `<QII>` record. Occurrence counts
remain fields in records and do not expand into repeated output records.

For capacity planning, let:

- `C = offtargetsCount * 8`, the complete catalogue size in bytes;
- `N`, the number of extractor partitions; and
- `B`, the largest selected raw bucket in bytes.

The largest catalogue partition is `8 * ceil(offtargetsCount / N)` bytes,
approximately `ceil(C / N)`, while the hard upper bound for one bucket's
hydrated output is `2B`. Because selected buckets are processed sequentially
and the bucket and output paths are reused, a conservative peak
ephemeral-storage estimate is:

```text
8 * ceil(offtargetsCount / N) + B + 2B + safetyMargin
= 8 * ceil(offtargetsCount / N) + 3B + safetyMargin
```

The actual hydrated output is normally smaller because an extractor writes
only IDs in its assigned interval, but allocation must not assume that bucket
IDs are evenly distributed between intervals. Increasing `N` reduces the
catalogue term and usually the output term; it cannot reduce the full raw
bucket that every extractor must inspect.

The dynamic allocator therefore distinguishes two cases:

- `RETRY_WITH_MORE_EXTRACTORS`: the bucket fits, but the catalogue partition
  or estimated output requires a larger `N`.
- `BUCKET_EXCEEDS_LAMBDA_LIMIT`: `3B + safetyMargin` is already at or above the
  configured safe working limit, so increasing `N` cannot make the task safe.

The Dispatcher can calculate this before publishing extraction work because
`shards.json` contains `offtargetsCount` and every bucket's byte boundaries.
Runtime Extractor logs should report the actual catalogue, bucket, output, and
combined local byte counts so the conservative estimate can be compared with
production behaviour. This model primarily protects Lambda `/tmp`; memory-map
residency and native process overhead still require a safety margin against the
Lambda memory limit.
