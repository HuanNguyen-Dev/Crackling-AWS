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

The source implementation used for this binary is
`isslScoreOfftargetsMapper.cpp`. Its build command is:

```text
sudo docker run --rm \
  --platform linux/amd64 \
  --entrypoint /bin/bash \
  -v "$PWD:/src" \
  -w /src \
  public.ecr.aws/lambda/python:3.10 \
  -lc '
    yum install -y gcc-c++ libgomp &&
    g++ -o mapper \
      isslScoreOfftargetsMapper.cpp \
      -O3 \
      -std=c++11 \
      -fopenmp \
      -mpopcnt \
      -Iinclude \
      -static-libgcc \
      -static-libstdc++
  '
```

The precompiled `mapper` binary was introduced into this AWS repository in
commit `e3a35b2` (`Feature: Add memory-aware shard mapper outputs`). Compilation
was performed outside this repository from the supplied local
`isslScoreOfftargetsMapper.cpp`; that source directory is:
https://github.com/HuanNguyen-Dev/Crackling/commit/65e5b69736d3f88ee17a5d63dfdeb0f797b38496,
in the commit `65e5b69736d3f88ee17a5d63dfdeb0f797b38496`.

The Mapper's native combined output is an invocation-local intermediate. The
wrapper streams it into separate compact MIT and CFD objects containing packed
little-endian `(uint32 targetId, float64 contribution)` records. Stage 3 can
deduplicate target IDs across shards before calculating each final score.
