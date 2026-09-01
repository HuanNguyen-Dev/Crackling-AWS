# ISSL Reducer Lambda

The Reducer completes Stage 3 of the distributed off-target scoring path. S3
invokes it whenever an object ending in `mapper-result.json` is created. Only Mapper
completion keys matching the expected job, target, and shard structure are
processed; Reducer completion events and unrelated objects are ignored.

## Completion gate

For each candidate guide, the Reducer checks for all five Mapper markers:

```text
<genome>/mapper/<job-id>/targets/<target-id>/shards/0/mapper-result.json
...
<genome>/mapper/<job-id>/targets/<target-id>/shards/4/mapper-result.json
```

An invocation exits successfully when any marker is still absent. S3 invokes
the function again as later markers arrive, so the event associated with the
last completed shard can begin reduction. Duplicate and out-of-order events
are safe.

## Reduction

Each Mapper produces separate MIT and CFD streams of packed little-endian
`(uint32 targetId, float64 contribution)` records. The Reducer streams all ten
objects into a temporary SQLite database keyed by off-target ID. This keeps
Lambda memory bounded and removes targets found through more than one ISSL
slice. Duplicate contributions must agree; conflicting values fail visibly.

Final scores match the local C++ Reducer:

```text
score = 10000 / (100 + sum of unique contributions)
```

## Outputs

The Reducer writes:

```text
<genome>/reducer/<job-id>/targets/<target-id>/mit.json
<genome>/reducer/<job-id>/targets/<target-id>/cfd.json
<genome>/reducer/<job-id>/targets/<target-id>/result.json
```

The MIT and CFD documents contain the final score, contribution sum, unique
off-target count, source-record count, and duplicate count. `result.json` is an
internal completion manifest referencing both score documents; it is not a
user-facing list of off-target IDs.

Before writing the final marker, one DynamoDB transaction:

- stores the MIT score in the existing target `IsslScore` field;
- stores CFD in `CfdScore` for later API integration;
- records a deterministic `ReducerTaskId`; and
- increments `NumScoredOfftarget` and the task-tracking version once.

This preserves the existing job-completion and results pipeline. Retries that
find the same `ReducerTaskId` do not increment the counter again.
