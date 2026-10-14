# ISSL batch result finalizer

The Reducer persists final MIT/CFD scores calculated by the combined Mapper.
It consumes an S3 notification, directly or through the existing SQS envelope,
for this schema-7 batch result:

```text
<genome>/mapper/<job-id>/batches/<batch-id>/mapper-result.json
```

Old per-guide/shard paths and unrelated objects are ignored. A result on the
batch path with an incompatible schema fails rather than mixing scoring formats.
The Extractor and Extraction Completion interfaces are unchanged.

## Validation and persistence

The complete result must have matching job, batch and genome identities,
`status: "complete"`, valid scoring settings and a nonempty score list with
unique nonnegative Target IDs. Enabled scores must be finite within 0..100;
disabled MIT/CFD scores must be `-1`. `scoreSemantics` is `complete` at threshold
zero and `thresholded` at a positive threshold. Every row is validated before
any target is written. Thresholded values can be early-stop scores.

For each guide, the existing two-item DynamoDB transaction atomically:

- stores `IsslScore`, `CfdScore` and `ReducerTaskId` on the existing target;
- increments `TaskTracking.NumScoredOfftarget` and `Version` once.

The completion identity remains `<job-id>:<target-id>`. Conditional guards
require an existing target without `ReducerTaskId` and an existing tracking
item. Transaction conflicts use bounded exponential backoff with jitter.
On a conditional failure, a consistent target read recognizes an identical
completed score pair; a conflicting identity or score remains an error.

The Reducer performs no candidate downloads, off-target deduplication,
contribution summation or SQLite reduction. CDK configures 4,096 MB memory
and default ephemeral storage for this finalizer.

## Completion and retries

After every target has been persisted or recognized as already complete,
the Reducer writes one final marker:

```text
<genome>/reducer/<job-id>/batches/<batch-id>/result.json
```

It includes schema/status, job/batch/genome identities, `sourceMapperKey`,
`guideCount`, scoring settings, score semantics and `sourceDigest`, the SHA256
of the canonical Mapper result JSON. A matching final marker skips further
DynamoDB work; changed source content or a conflicting marker fails.

A failure during target persistence or final-marker upload leaves the batch
retryable. Completed targets are skipped on retry without advancing counters
again. SQS delivery returns `batchItemFailures` for failed messages while
allowing other messages to succeed; direct S3 failures propagate.
