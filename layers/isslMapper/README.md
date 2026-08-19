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
g++ -o mapper isslScoreOfftargetsMapper.cpp -O3 -std=c++11 -fopenmp -mpopcnt -Iinclude
```

The precompiled `mapper` binary was introduced into this AWS repository in
commit `e3a35b2` (`Feature: Add memory-aware shard mapper outputs`). Compilation
was performed outside this repository from the supplied local
`isslScoreOfftargetsMapper.cpp`; that source directory did not include Git
metadata identifying a separate compilation-source commit, so no unverifiable
source revision is claimed here.

The Mapper's native combined output is an invocation-local intermediate. The
wrapper streams it into separate compact MIT and CFD objects containing packed
little-endian `(uint32 targetId, float64 contribution)` records. Stage 3 can
deduplicate target IDs across shards before calculating each final score.
