# Layer: `isslMapper`

This layer supplies the Linux x86_64 AVX2 scorer at `/opt/mapper`. Schema-7
Mapper tasks combine the selected hydrated buckets from all index slices for
one guide batch. The Dispatcher supplies each guide's bucket indexes, so the
Mapper downloads each required bucket once and each guide scans only its own
selected ranges. The initial batch cap is 100 guides, with a separate bound on
the total hydrated input bytes. The Extractor and cache format stay unchanged.

**Rebuild the binary before using the schema-7 wrapper.** The existing packaged
binary implements an incompatible shard/contribution interface. No binary was
built or packaged as part of this revision. Build from the updated local source
`ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2.cpp`; the historical
[CracklingPlusPlus commit `74a4561`](https://github.com/bmds-lab/CracklingPlusPlus/commit/74a4561a4a503612d826419325a85dfc3f87e15a)
does not itself provide the schema-7 interface.

Before building, change that target's CMake options from `-march=native` to
explicit Lambda-compatible instructions:

```cmake
target_compile_options(ISSLScoreOfftargetsMMF_AVX2 PRIVATE
    -O3
    -mavx2
    -mpopcnt
)
```

Run the following from the root of the checked-out CracklingPlusPlus source
tree. It builds only the AVX2 Mapper target and its `utils` and
`otScorePenalties` dependencies, then writes the deployable binary to
`bin/mapper` at that source-tree root:

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$PWD:/src" \
  -w /src \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y \
      gcc10 gcc10-c++ gcc10-binutils \
      binutils make cmake3 boost-devel libgomp &&
    rm -rf build-lambda &&
    cmake3 -S . -B build-lambda \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc10-gcc \
      -DCMAKE_CXX_COMPILER=gcc10-g++ \
      -DCMAKE_AR=/usr/bin/ar &&
    cmake3 --build build-lambda \
      --target ISSLScoreOfftargetsMMF_AVX2 \
      --parallel "$(nproc)" &&
    mkdir -p /src/bin &&
    cp build-lambda/ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2 \
      /src/bin/mapper &&
    chmod 755 /src/bin/mapper
  '
```

Copy the resulting `bin/mapper` into this layer's deployment contents at
`layers/isslMapper/mapper` before packaging. The wrapper copies `/opt/mapper`
to `/tmp/mapper` and invokes it with exactly seven positional arguments:

```text
mapper candidates.bin query.txt bucket-plan.txt idBits maxDistance scoreThreshold scoreMethod
```

`candidates.bin` contains the existing packed little-endian hydrated records:
`signature:uint64`, `globalOfftargetId:uint32-or-uint64`, `occurrences:uint32`.
The record width is 16 bytes for 32-bit IDs and 20 bytes for 64-bit IDs.
`query.txt` has one 20-base A/C/G/T guide per line. The bucket plan contains
unsigned integers with this structure:

```text
bucketCount
compactOffsetBytes candidateCount          # one row per bucket
queryCount
selectedBucketCount bucketIndex...         # one row per query
```

Bucket indexes refer to the task's bucket vector; they are not off-target IDs.
Preserve the supplied bucket order, each guide's slice order and fragment
record order. Empty buckets retain their vector entry. Off-target IDs are
deduplicated locally per guide across its selected ranges, retaining sequence
and occurrence information. Guides remain parallel through OpenMP; the wrapper
and Lambda configuration leave thread-count selection to the OpenMP runtime.

The native process writes only final score rows to stdout, in query order:

```text
guideSequence<TAB>MIT<TAB>CFD
```

Disabled score methods use `-1`; enabled scores are finite values in 0..100.
Emit enough precision to round-trip a double. Write diagnostics to stderr and
return a nonzero exit status on failure. There is no output-prefix argument.

Threshold zero disables early stopping and produces complete scores over the
selected ranges. A positive threshold enables the reference stopping rules;
those scores may represent stopping points and the wrapper labels the batch
`scoreSemantics: "thresholded"`. It validates every row before publishing one
schema-7 `mapper-result.json` for the whole batch. The Reducer persists these
final scores and progress, without contribution downloads or deduplication.

The wrapper streams downloads with `MAPPER_DOWNLOAD_WORKERS` (default 4,
range 1..16), validates the manifests and planned hydrated bytes, and stages at
most two copies of the hydrated input during concatenation. Native stdout and
stderr go to temporary files; the process timeout uses the remaining Lambda
time with a 15-second margin. Actual peak memory and duration still require
the rice and mistletoe deployment benchmarks.

## Runtime shared libraries

The AVX2 Mapper dynamically links Amazon Linux 2's Boost Regex, Boost
Iostreams, and OpenMP runtime libraries. Package the matching copies in this
repository's shared-library layer; do not copy Ubuntu/WSL libraries.

From the Crackling-AWS repository root, run:

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$PWD/layers/lib/lib:/out" \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y boost-regex boost-iostreams libgomp &&
    cp -v \
      /usr/lib64/libboost_regex-mt.so.1.53.0 \
      /usr/lib64/libboost_iostreams-mt.so.1.53.0 \
      /usr/lib64/libgomp.so.1 \
      /out/
  '
```

The target directory is deployed at `/opt/lib`. The Mapper Lambda already sets
`LD_LIBRARY_PATH` to include that path. Verify the binary in the matching
container (without overriding its system-library search path):

```bash
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$HOME/CracklingPlusPlus/bin/mapper:/mapper:ro" \
  -v "$PWD/layers/lib/lib:/opt/lib:ro" \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    export LD_LIBRARY_PATH=/lib64:/usr/lib64:/opt/lib &&
    ldd -v /mapper | tee /tmp/mapper-ldd.txt &&
    ! grep -q "not found" /tmp/mapper-ldd.txt
  '
```

This checks against the libraries actually packaged in `layers/lib`; it does
not install Boost or OpenMP into the test container and mask a missing layer
dependency. The output must contain no `not found` entries, including missing
versioned symbols, before deployment. The Mapper function must be attached to
both the `isslMapper` binary layer and the shared-library layer.
