import hashlib
import itertools
import json
import os

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ['BUCKET']
MAPPER_QUEUE = os.environ['MAPPER_QUEUE']
EXTRACTOR_QUEUE = os.environ['EXTRACTOR_QUEUE']
SHARD_COUNT = int(os.getenv('NUM_SHARDS', '5'))
EXTRACTOR_COUNT = int(os.getenv('EXTRACTOR_COUNT', '5'))
MAX_GUIDES = int(os.getenv('MAX_GUIDES_PER_GROUP', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '75'))
SCORE_METHOD = os.getenv('SCORE_METHOD', 'and')
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

if EXTRACTOR_COUNT < 1 or MAX_GUIDES < 1:
    raise ValueError('Extractor count and maximum guide batch size must be positive')


def _json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return None
        raise


def _hash(*parts):
    return hashlib.sha256(':'.join(map(str, parts)).encode()).hexdigest()


def _signature(sequence):
    value = 0
    sequence = sequence[:20].upper()
    if len(sequence) != 20 or any(base not in 'ACGT' for base in sequence):
        raise ValueError('Guide must begin with exactly 20 A/C/G/T bases')
    for index, base in enumerate(sequence):
        value |= 'ACGT'.index(base) << (index * 2)
    return value


def _bucket(sequence, slice_id, layout):
    return (_signature(sequence) >> (slice_id * int(layout['sliceWidth']))) & (
        int(layout['sliceLimit']) - 1)


def _send(queue, tasks):
    for batch_start in range(0, len(tasks), 10):
        batch = tasks[batch_start:batch_start + 10]
        response = sqs.send_message_batch(QueueUrl=queue, Entries=[
            {'Id': str(batch_start + index),
             'MessageBody': json.dumps(task, separators=(',', ':'))}
            for index, task in enumerate(batch)
        ])
        if response.get('Failed') or len(response.get('Successful', [])) != len(batch):
            raise RuntimeError('SQS did not accept every task')


def _mapper_tasks(guides, genome, manifest, selected):
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    job_id = str(guides[0]['JobID'])
    tasks = []
    for shard in manifest['shards']:
        slice_id = int(shard['sliceId'])
        bucket_refs = [
            {'bucketId': item['bucketId'], 'manifestKey': item['manifestKey']}
            for item in selected if item['sliceId'] == slice_id
        ]
        contracts = []
        for guide in guides:
            target_id = int(guide['TargetID'])
            bucket_id = _bucket(guide['Sequence'], slice_id, manifest['layout'])
            prefix = f'{genome}/mapper/{job_id}/targets/{target_id}/shards/{slice_id}'
            contracts.append({
                'taskId': _hash(job_id, target_id, slice_id),
                'targetId': target_id, 'guideSequence': guide['Sequence'],
                'bucketId': bucket_id,
                'output': {'mitKey': f'{prefix}/mit.bin', 'cfdKey': f'{prefix}/cfd.bin',
                           'metadataKey': f'{prefix}/mapper-result.json'},
            })
        tasks.append({
            'schemaVersion': 5,
            'batchId': _hash(job_id, *(g['TargetID'] for g in guides), slice_id),
            'jobId': job_id, 'genome': genome, 'guides': contracts,
            'shardId': int(shard['shardId']), 'shardCount': SHARD_COUNT,
            'sliceId': slice_id, 'buckets': bucket_refs,
            'scoringMetadata': {
                'bucket': manifest['issl']['bucket'], 'key': manifest['issl']['key'],
                'endByte': 48 + int(manifest['layout']['scoresCount']) * 16,
            },
            'scoring': {
                'maxDistance': MAX_DISTANCE,
                'scoreThreshold': SCORE_THRESHOLD,
                'scoreMethod': SCORE_METHOD,
            },
            'output': {'bucket': BUCKET},
        })
    return tasks


def _group_bytes(guides, manifest):
    total = 0
    for shard in manifest['shards']:
        offsets = shard['bucketOffsets']
        bucket_ids = {
            _bucket(guide['Sequence'], int(shard['sliceId']), manifest['layout'])
            for guide in guides
        }
        # Original buckets contain 8-byte ID/occurrence entries. Hydrated
        # candidate records are 16 bytes, so their exact projected size is
        # twice the selected source-bucket bytes.
        total += sum(
            (int(offsets[bucket_id + 1]) - int(offsets[bucket_id])) * 2
            for bucket_id in bucket_ids
        )
    return total


def _partition_guides(guides, manifest):
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    if len(guides) <= MAX_GUIDES:
        return [guides]

    groups = []
    remaining = guides
    while len(remaining) > MAX_GUIDES:
        # Dispatcher SQS events currently contain at most ten guides. Keep the
        # exhaustive locality choice used by the bucket-selective pipeline for
        # that common case, and fall back to deterministic chunks if the event
        # source batch is increased substantially in the future.
        if len(remaining) <= MAX_GUIDES * 2 and len(remaining) <= 20:
            candidates = []
            for first_size in range(1, MAX_GUIDES + 1):
                second_size = len(remaining) - first_size
                if not 1 <= second_size <= MAX_GUIDES:
                    continue
                for tail in itertools.combinations(
                        range(1, len(remaining)), first_size - 1):
                    indexes = {0, *tail}
                    first = [
                        item for index, item in enumerate(remaining)
                        if index in indexes
                    ]
                    second = [
                        item for index, item in enumerate(remaining)
                        if index not in indexes
                    ]
                    tie = tuple(int(item['TargetID']) for item in first)
                    candidates.append((max(_group_bytes(first, manifest),
                                           _group_bytes(second, manifest)), tie,
                                       first, second))
            _, _, first, second = min(candidates, key=lambda item: (item[0], item[1]))
            groups.extend((first, second))
            return groups
        groups.append(remaining[:MAX_GUIDES])
        remaining = remaining[MAX_GUIDES:]
    if remaining:
        groups.append(remaining)
    return groups


def _dispatch_group(guides, genome, manifest):
    selected = []
    for shard in manifest['shards']:
        slice_id = int(shard['sliceId'])
        offsets = shard['bucketOffsets']
        for bucket_id in sorted({_bucket(g['Sequence'], slice_id, manifest['layout']) for g in guides}):
            prefix = f'{genome}/issl/cache/slices/{slice_id}/buckets/{bucket_id}'
            manifest_key = f'{prefix}/manifest.json'
            selected.append({
                'sliceId': slice_id, 'bucketId': bucket_id,
                'startByte': int(offsets[bucket_id]), 'endByte': int(offsets[bucket_id + 1]),
                'cachePrefix': prefix, 'manifestKey': manifest_key,
                'cached': _json(manifest_key) is not None,
            })
    mapper_tasks = _mapper_tasks(guides, genome, manifest, selected)
    missing = [{key: value for key, value in item.items() if key != 'cached'}
               for item in selected if not item['cached']]
    if not missing:
        _send(MAPPER_QUEUE, mapper_tasks)
        return

    job_id = str(guides[0]['JobID'])
    batch_id = _hash(
        job_id,
        *(int(g['TargetID']) for g in guides),
        f'extractors={EXTRACTOR_COUNT}',
    )
    prefix = f'{genome}/issl/extractions/{job_id}/{batch_id}'
    batch_key = f'{prefix}/batch.json'
    count = int(manifest['layout']['offtargetsCount'])
    batch = {
        'schemaVersion': 1,
        'batchId': batch_id,
        'expectedParts': EXTRACTOR_COUNT,
        'offtargetsCount': count,
        'missingBuckets': missing,
        'mapperTasks': mapper_tasks,
    }
    s3.put_object(Bucket=BUCKET, Key=batch_key,
                  Body=json.dumps(batch, separators=(',', ':')).encode(),
                  ContentType='application/json')
    catalogue_start = 48 + int(manifest['layout']['scoresCount']) * 16
    tasks = []
    for part_id in range(EXTRACTOR_COUNT):
        start_id = part_id * count // EXTRACTOR_COUNT
        end_id = (part_id + 1) * count // EXTRACTOR_COUNT
        tasks.append({
            'schemaVersion': 1, 'batchId': batch_id, 'batchKey': batch_key,
            'partId': part_id, 'expectedParts': EXTRACTOR_COUNT,
            'idRange': {'start': start_id, 'end': end_id},
            'catalogue': {
                'bucket': manifest['issl']['bucket'],
                'key': manifest['issl']['key'],
                'startByte': catalogue_start + start_id * 8,
                'endByte': catalogue_start + end_id * 8,
            },
            'buckets': missing, 'completionPrefix': f'{prefix}/parts',
        })
    _send(EXTRACTOR_QUEUE, tasks)


def lambda_handler(event, context):
    grouped = {}
    for record in event.get('Records', []):
        body = json.loads(record['body'])
        guide = json.loads(body['default'])
        genome = str(json.loads(body['genome']))
        target_id = int(guide['TargetID'])
        unique = grouped.setdefault((str(guide['JobID']), genome), {})
        if target_id in unique and unique[target_id] != guide:
            raise ValueError(f'Conflicting duplicate target ID: {target_id}')
        unique[target_id] = guide
    batches = 0
    for (_, genome), guides_by_id in grouped.items():
        manifest = _json(f'{genome}/issl/shards.json')
        if not manifest or manifest.get('schemaVersion') != 2:
            raise ValueError('Missing or unsupported ISSL shard manifest')
        guides = sorted(guides_by_id.values(), key=lambda item: int(item['TargetID']))
        for group in _partition_guides(guides, manifest):
            _dispatch_group(group, genome, manifest)
            batches += 1
    return {'processedGuides': len(event.get('Records', [])), 'dispatchedBatches': batches}
