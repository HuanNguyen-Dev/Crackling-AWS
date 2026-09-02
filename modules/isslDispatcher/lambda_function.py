import hashlib
import itertools
import json
import os

import boto3


BUCKET = os.environ['BUCKET']
MAPPER_QUEUE_URL = os.environ['MAPPER_QUEUE']
NUM_SHARDS = int(os.getenv('NUM_SHARDS', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '75'))
SCORE_METHOD = os.getenv('SCORE_METHOD', 'and')
MAX_GUIDES_PER_GROUP = 5
RANGE_MERGE_GAP_BYTES = 8 * 1024 * 1024

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def _parse_guide(record):
    body = json.loads(record['body'])
    guide = json.loads(body['default'])
    genome = json.loads(body['genome'])

    for field in ('Sequence', 'JobID', 'TargetID'):
        if field not in guide:
            raise ValueError(f'Guide message is missing {field}')

    return guide, str(genome)


def _load_manifest(genome):
    key = f'{genome}/issl/shards.json'
    response = s3_client.get_object(Bucket=BUCKET, Key=key)
    manifest = json.loads(response['Body'].read())
    if manifest.get('schemaVersion') != 2:
        raise ValueError('Unsupported or missing shard manifest schemaVersion')
    if int(manifest.get('maxDistance', -1)) != MAX_DISTANCE or MAX_DISTANCE != 4:
        raise ValueError('Shard manifest and Dispatcher must use MAX_DISTANCE=4')
    shards = manifest.get('shards', [])
    if len(shards) != NUM_SHARDS:
        raise ValueError(
            f'Expected {NUM_SHARDS} shards in s3://{BUCKET}/{key}, '
            f'found {len(shards)}'
        )
    slice_limit = int(manifest['layout']['sliceLimit'])
    for shard in shards:
        if len(shard.get('bucketOffsets', [])) != slice_limit + 1:
            raise ValueError('Shard manifest has invalid bucket offsets')
    return key, manifest


def _sequence_to_signature(sequence):
    nucleotide_index = {'A': 0, 'C': 1, 'G': 2, 'T': 3}
    sequence = sequence[:20].upper()
    if len(sequence) != 20 or any(base not in nucleotide_index for base in sequence):
        raise ValueError('Each guide must begin with exactly 20 A/C/G/T bases')
    signature = 0
    for position, nucleotide in enumerate(sequence):
        signature |= nucleotide_index[nucleotide] << (position * 2)
    return signature


def _bucket_id(sequence, slice_id, slice_width, slice_limit):
    signature = _sequence_to_signature(sequence)
    return (signature >> (slice_id * slice_width)) & (slice_limit - 1)


def _selected_buckets(guides, manifest, shard):
    layout = manifest['layout']
    slice_id = int(shard['sliceId'])
    offsets = shard['bucketOffsets']
    bucket_ids = sorted({
        _bucket_id(
            guide['Sequence'], slice_id, int(layout['sliceWidth']),
            int(layout['sliceLimit']),
        )
        for guide in guides
    })
    return [
        {
            'bucketId': bucket_id,
            'startByte': int(offsets[bucket_id]),
            'endByte': int(offsets[bucket_id + 1]),
        }
        for bucket_id in bucket_ids
    ]


def _merge_bucket_ranges(buckets):
    nonempty = [item for item in buckets if item['endByte'] > item['startByte']]
    ranges = []
    for bucket in sorted(nonempty, key=lambda item: item['startByte']):
        if ranges and bucket['startByte'] - ranges[-1]['endByte'] <= RANGE_MERGE_GAP_BYTES:
            ranges[-1]['endByte'] = max(ranges[-1]['endByte'], bucket['endByte'])
            ranges[-1]['bucketIds'].append(bucket['bucketId'])
        else:
            ranges.append({
                'startByte': bucket['startByte'],
                'endByte': bucket['endByte'],
                'bucketIds': [bucket['bucketId']],
            })
    return ranges


def _invocation_bytes(guides, manifest, shard):
    ranges = _merge_bucket_ranges(_selected_buckets(guides, manifest, shard))
    return int(manifest['layout']['baseOffsetBytes']) + sum(
        item['endByte'] - item['startByte'] for item in ranges
    )


def _partition_guides(guides, manifest):
    guides = sorted(guides, key=lambda item: int(item['TargetID']))
    if len(guides) <= MAX_GUIDES_PER_GROUP:
        return [guides]
    if len(guides) > MAX_GUIDES_PER_GROUP * 2:
        raise ValueError('Dispatcher event contains more than ten guides for one job')

    # Keeping the first guide in the first group removes mirrored partitions.
    tail_indexes = range(1, len(guides))
    candidates = []
    for first_size in range(1, MAX_GUIDES_PER_GROUP + 1):
        second_size = len(guides) - first_size
        if not 1 <= second_size <= MAX_GUIDES_PER_GROUP:
            continue
        for chosen_tail in itertools.combinations(tail_indexes, first_size - 1):
            first_indexes = {0, *chosen_tail}
            first = [guide for index, guide in enumerate(guides) if index in first_indexes]
            second = [guide for index, guide in enumerate(guides) if index not in first_indexes]
            largest = max(
                _invocation_bytes(group, manifest, shard)
                for group in (first, second)
                for shard in manifest['shards']
            )
            tie_break = tuple(tuple(int(g['TargetID']) for g in group) for group in (first, second))
            candidates.append((largest, tie_break, [first, second]))
    return min(candidates, key=lambda item: (item[0], item[1]))[2]


def _guide_task_id(job_id, target_id, shard_id):
    value = f'{job_id}:{target_id}:{shard_id}'.encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def _batch_id(job_id, target_ids, shard_id):
    joined_targets = ','.join(str(target_id) for target_id in target_ids)
    value = f'{job_id}:{joined_targets}:{shard_id}'.encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def _guide_contract(guide, genome, shard_id, bucket_id):
    job_id = str(guide['JobID'])
    target_id = int(guide['TargetID'])
    output_prefix = (
        f'{genome}/mapper/{job_id}/targets/{target_id}/shards/{shard_id}'
    )
    return {
        'taskId': _guide_task_id(job_id, target_id, shard_id),
        'targetId': target_id,
        'guideSequence': guide['Sequence'],
        'bucketId': bucket_id,
        'output': {
            'prefix': output_prefix,
            'mitKey': f'{output_prefix}/mit.bin',
            'cfdKey': f'{output_prefix}/cfd.bin',
            'metadataKey': f'{output_prefix}/mapper-result.json',
        },
    }


def _mapper_task(guides, genome, manifest_key, manifest, shard):
    job_id = str(guides[0]['JobID'])
    shard_id = int(shard['shardId'])
    selected_buckets = _selected_buckets(guides, manifest, shard)
    buckets_by_id = {item['bucketId']: item for item in selected_buckets}
    layout = manifest['layout']
    guide_contracts = [
        _guide_contract(
            guide, genome, shard_id,
            _bucket_id(
                guide['Sequence'], int(shard['sliceId']),
                int(layout['sliceWidth']), int(layout['sliceLimit']),
            ),
        )
        for guide in sorted(guides, key=lambda item: int(item['TargetID']))
    ]

    return {
        'schemaVersion': 4,
        'batchId': _batch_id(
            job_id,
            [guide['targetId'] for guide in guide_contracts],
            shard_id,
        ),
        'jobId': job_id,
        'genome': genome,
        'guides': guide_contracts,
        'shardId': shard_id,
        'shardCount': NUM_SHARDS,
        'sliceId': int(shard['sliceId']),
        'selectedBuckets': [buckets_by_id[key] for key in sorted(buckets_by_id)],
        'ranges': _merge_bucket_ranges(selected_buckets),
        'issl': manifest['issl'],
        'manifest': {'bucket': BUCKET, 'key': manifest_key},
        'scoring': {
            'maxDistance': MAX_DISTANCE,
            'scoreThreshold': SCORE_THRESHOLD,
            'scoreMethod': SCORE_METHOD,
        },
        'output': {
            'bucket': BUCKET,
        },
    }


def _dispatch(guides, genome):
    manifest_key, manifest = _load_manifest(genome)
    groups = _partition_guides(guides, manifest)
    tasks = [
        _mapper_task(group, genome, manifest_key, manifest, shard)
        for group in groups for shard in manifest['shards']
    ]
    response = sqs_client.send_message_batch(
        QueueUrl=MAPPER_QUEUE_URL,
        Entries=[
            {'Id': f'{index}-{task["shardId"]}', 'MessageBody': json.dumps(task)}
            for index, task in enumerate(tasks)
        ],
    )
    if response.get('Failed'):
        raise RuntimeError(
            f'Failed to publish Mapper tasks: {json.dumps(response["Failed"])}'
        )
    if len(response.get('Successful', [])) != len(tasks):
        raise RuntimeError('Mapper fan-out did not publish every task')

    print(json.dumps({
        'event': 'guide_batch_dispatched',
        'jobId': guides[0]['JobID'],
        'targetIds': [int(guide['TargetID']) for guide in guides],
        'genome': genome,
        'batchIds': [task['batchId'] for task in tasks],
        'guideGroups': [
            [int(guide['TargetID']) for guide in group] for group in groups
        ],
    }))


def lambda_handler(event, context):
    groups = {}
    for record in event.get('Records', []):
        guide, genome = _parse_guide(record)
        key = (str(guide['JobID']), genome)
        groups.setdefault(key, []).append(guide)

    for (_, genome), guides in groups.items():
        unique = {}
        for guide in guides:
            target_id = int(guide['TargetID'])
            if target_id in unique and unique[target_id] != guide:
                raise ValueError(f'Conflicting duplicate target ID: {target_id}')
            unique[target_id] = guide
        _dispatch(list(unique.values()), genome)

    return {
        'processedGuides': len(event.get('Records', [])),
        'dispatchedBatches': len(groups),
    }
