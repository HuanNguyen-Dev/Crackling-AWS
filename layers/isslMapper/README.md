# Layer: `isslMapper`

This layer contains the precompiled Linux x86_64 Mapper from the local
Coordinator/Mapper/Reducer implementation supplied for the Crackling AWS
redesign. The Lambda wrapper supplies one guide and one Coordinator shard per
invocation.

The source implementation used for this binary is
`isslScoreOfftargetsMapper.cpp`. Its build command is:

```text
g++ -o mapper isslScoreOfftargetsMapper.cpp -O3 -std=c++11 -fopenmp -mpopcnt -Iinclude
```

The Mapper's native combined output is an invocation-local intermediate. The
wrapper streams it into separate compact MIT and CFD objects containing packed
little-endian `(uint32 targetId, float64 contribution)` records. Stage 3 can
deduplicate target IDs across shards before calculating each final score.
