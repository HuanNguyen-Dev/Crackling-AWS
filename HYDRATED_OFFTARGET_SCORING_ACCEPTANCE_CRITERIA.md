# Hydrated Off-Target Scoring Acceptance Criteria

## Objective

Increase the largest complete ISSL that the Lambda scoring pipeline can process
by removing the monolithic off-target signature catalogue from every Mapper.
Scoring completeness and the existing five-slice search semantics are retained.

## Architecture

1. The Dispatcher groups at most `MAX_GUIDES_PER_GROUP` guides and calculates
   their selected bucket in each of the five slices.
2. Hydrated bucket manifests already present under
   `<genome>/issl/cache/` are reused across jobs.
3. If selected buckets are not cached, the Dispatcher publishes
   `EXTRACTOR_COUNT` tasks. Each task owns a disjoint half-open global ID range
   and processes all missing selected buckets across all five slices.
4. Candidate Extractors read one contiguous catalogue partition, filter the
   selected bucket entries to their ID range, and publish compact candidate
   fragments.
5. After every extractor completion marker exists, bucket manifests are
   published and exactly five slice-specific Mapper tasks are released.
6. Each Mapper concatenates only its assigned slice's selected hydrated bucket
   fragments, memory-maps the compact file, and emits contributions containing
   the original global off-target IDs.
7. The existing Reducer deduplicates cross-slice results and calculates final
   MIT and CFD scores.

Only Lambda is introduced as compute. SQS carries control messages and S3 holds
the ISSL, cache, coordination documents, and scoring results.

## Binary contracts

Hydrated candidate files are headerless, fixed-width, little-endian records:

```text
signature:uint64 globalId:uint32 occurrences:uint32
```

The Python equivalent is `<QII`, exactly 16 bytes. Bucket manifests list an
arbitrary number of fragments so caches created with different configured
extractor counts remain readable.

Mapper results retain the existing 32-byte native record and downstream
12-byte `<Id` score-contribution formats.

## Required behaviour

- [ ] The original monolithic `.issl` format is unchanged.
- [ ] Every guide selects exactly one correct bucket in each of five slices.
- [ ] Guide groups contain no more than the configured maximum, initially five.
- [ ] Exactly five logical Mapper tasks are produced per guide group.
- [ ] Extractor ID ranges are disjoint and collectively cover
      `[0, offtargetsCount)` without gaps.
- [ ] The initial extractor count is five and can be changed for later
      hydration batches.
- [ ] Only selected cache-miss buckets are hydrated.
- [ ] Hydrated bucket manifests are shared by later jobs for the same genome.
- [ ] A bucket manifest is written only after every expected fragment is
      accounted for.
- [ ] Empty fragments make no invalid S3 request and are represented by a zero
      record count.
- [ ] Concurrent hydration of the same bucket produces deterministic fragment
      keys and byte-equivalent content.
- [ ] Every hydrated record preserves its exact signature, global ID, and
      occurrence count.
- [ ] Each Mapper downloads only scoring metadata and hydrated candidates for
      its assigned slice's selected buckets.
- [ ] No Mapper downloads or materialises the global signature catalogue.
- [ ] The native Mapper performs no allocation proportional to the complete
      global off-target count.
- [ ] Multiple guides selecting the same bucket reuse its materialised bytes.
- [ ] Global IDs reach the Reducer unchanged so cross-slice duplicates are
      counted once.
- [ ] Retries use deterministic batch, fragment, Mapper, and output identities.
- [ ] Extraction completion markers from a different extractor partitioning
      scheme cannot be mixed into one cache manifest.

## Equivalence and capacity

- [ ] Hydrated scoring produces MIT and CFD final scores within an absolute
      tolerance of `0.000001` of the original whole-index scorer.
- [ ] Equivalence covers exact matches, mismatches through distance four,
      occurrence counts greater than one, empty buckets, shared guide buckets,
      and candidates discovered through multiple slices.
- [ ] A complete ISSL larger than the previously supported index completes
      without sampling, deleting, or capping off-target candidates.
- [ ] Observed Mapper memory and materialised input are driven by selected
      hydrated bucket records rather than total catalogue size.
- [ ] A cache hit avoids rerunning catalogue extraction for that bucket.

Bucket subdivision, pathological bucket skew, cache expiry, DLQs, mutable-ISSL
version handling, and changing the existing global-ID width are out of scope for
the first implementation.

## Native build handoff

The native sources are maintained in the separate Crackling scoring repository:

```text
Crackling/src/ISSL/isslExtractCandidates.cpp
Crackling/src/ISSL/isslScoreOfftargetsMapper.cpp
```

The project owner builds the Lambda-compatible binaries and places them at the
root of their respective layers as `extractor` and `mapper`.
