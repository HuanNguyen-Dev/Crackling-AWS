# Layer: `isslMapper`

This layer contains the precompiled Linux x86_64 Mapper from the local
Coordinator/Mapper/Reducer implementation supplied for the Crackling AWS
redesign. The Lambda wrapper supplies one batch of guides and one Coordinator
shard per invocation.

The wrapper now preserves the existing SQS batching boundary. The Dispatcher
groups the guide records delivered in one `sqsIssl` Lambda event by job and
genome, then publishes five Mapper tasks per group. Each Mapper invocation
therefore receives one shard and a query file containing up to the SQS event
batch size of 10 guides, rather than invoking five Mappers for every individual
guide.

The source implementation is the hydrated AVX2 scorer:
`ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2.cpp` from
[CracklingPlusPlus commit `74a4561`](https://github.com/bmds-lab/CracklingPlusPlus/commit/74a4561a4a503612d826419325a85dfc3f87e15a).

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
      make cmake3 boost-devel libgomp &&
    rm -rf build-lambda &&
    cmake3 -S . -B build-lambda \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=gcc10-gcc \
      -DCMAKE_CXX_COMPILER=gcc10-g++ &&
    cmake3 --build build-lambda \
      --target ISSLScoreOfftargetsMMF_AVX2 \
      --parallel "$(nproc)" &&
    mkdir -p /src/bin &&
    cp build-lambda/ISSLScoreOfftargetsMMF/ISSLScoreOfftargetsMMF_AVX2 \
      /src/bin/mapper &&
    chmod 755 /src/bin/mapper
  '
```

The Lambda wrapper invokes this binary with hydrated candidates, a query file,
a shard plan, ID width, and scoring settings. It does not supply a raw `.issl`
path.

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
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y boost-regex boost-iostreams libgomp &&
    ldd /mapper
  '
```

The `ldd` output must contain no `not found` entries before deployment.

The Mapper's native combined output is an invocation-local intermediate. The
wrapper streams it into separate compact MIT and CFD objects containing packed
little-endian `(uint32|uint64 targetId, float64 contribution)` records. Stage 3
can deduplicate target IDs across shards before calculating each final score.
