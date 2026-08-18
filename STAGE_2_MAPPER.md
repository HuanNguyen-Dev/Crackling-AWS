# Stage 2 Mapper Contract

Stage 2 replaces the original `isslScoreOfftargets` Lambda with a five-shard
Mapper path. It intentionally does not implement or invoke the Reducer.

## Event flow

1. ISSL Creation uploads the index and sends one Coordinator message.
2. The Coordinator writes `<genome>/issl/shards.json`, then sends the original
   job payload to the Target Scan queue.
3. Target Scan keeps its candidate-guide algorithm and existing `sqsIssl`
   message contract.
4. The Dispatcher consumes each guide and publishes five schema-version 2
   Mapper tasks, one for every manifest shard.
5. Each Mapper processes one guide against one shard and writes its partial
   output to deterministic S3 keys.

SQS delivery is at least once. Dispatcher task IDs and Mapper output keys are
derived from job ID, target ID, and shard ID, so retries replace the same
partial result. A Mapper writes `result.json` last and treats a matching marker
as successful completion on later delivery.

## S3 output

For target `<target-id>` and shard `<shard-id>`:

```text
<genome>/mapper/<job-id>/targets/<target-id>/shards/<shard-id>/mit.bin
<genome>/mapper/<job-id>/targets/<target-id>/shards/<shard-id>/cfd.bin
<genome>/mapper/<job-id>/targets/<target-id>/shards/<shard-id>/result.json
```

MIT and CFD objects contain packed, little-endian 12-byte records:

```text
uint32 targetId
float64 contribution
```

Zero contributions and the local Mapper's no-result sentinel are omitted.
`result.json` records counts, format details, correlation fields, and both
object keys. A future Reducer must deduplicate off-target IDs across the five
shards, sum the remaining contributions independently, and apply the final
score calculation.

## Memory and storage behavior

The Lambda does not download five full ISSL copies. For its assigned task it
uses S3 range requests to materialize only:

- the header, precomputed scores, off-target signatures, and slice-size table;
- the assigned shard's slice contents.

The gap between the common index data and shard bytes is sparse in `/tmp`.
Native Mapper output is streamed into separate MIT and CFD files rather than
loaded into Python memory.
