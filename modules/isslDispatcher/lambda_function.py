import hashlib
import json
import os

import boto3


BUCKET = os.environ['BUCKET']
MAPPER_QUEUE_URL = os.environ['MAPPER_QUEUE']
NUM_SHARDS = int(os.getenv('NUM_SHARDS', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '75'))
SCORE_METHOD = os.getenv('SCORE_METHOD', 'and')

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
    shards = manifest.get('shards', [])
    if len(shards) != NUM_SHARDS:
        raise ValueError(
            f'Expected {NUM_SHARDS} shards in s3://{BUCKET}/{key}, '
            f'found {len(shards)}'
        )
    return key, manifest


def _guide_task_id(job_id, target_id, shard_id):
    value = f'{job_id}:{target_id}:{shard_id}'.encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def _batch_id(job_id, target_ids, shard_id):
    joined_targets = ','.join(str(target_id) for target_id in target_ids)
    value = f'{job_id}:{joined_targets}:{shard_id}'.encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def _guide_contract(guide, genome, shard_id):
    job_id = str(guide['JobID'])
    target_id = int(guide['TargetID'])
    output_prefix = (
        f'{genome}/mapper/{job_id}/targets/{target_id}/shards/{shard_id}'
    )
    return {
        'taskId': _guide_task_id(job_id, target_id, shard_id),
        'targetId': target_id,
        'guideSequence': guide['Sequence'],
        'output': {
            'prefix': output_prefix,
            'mitKey': f'{output_prefix}/mit.bin',
            'cfdKey': f'{output_prefix}/cfd.bin',
            'metadataKey': f'{output_prefix}/result.json',
        },
    }


def _mapper_task(guides, genome, manifest_key, manifest, shard):
    job_id = str(guides[0]['JobID'])
    shard_id = int(shard['shardId'])
    guide_contracts = [
        _guide_contract(guide, genome, shard_id)
        for guide in sorted(guides, key=lambda item: int(item['TargetID']))
    ]

    return {
        'schemaVersion': 3,
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
        'startSlice': int(shard['startSlice']),
        'endSlice': int(shard['endSlice']),
        'startByte': int(shard['startByte']),
        'endByte': int(shard['endByte']),
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
    tasks = [
        _mapper_task(guides, genome, manifest_key, manifest, shard)
        for shard in manifest['shards']
    ]
    response = sqs_client.send_message_batch(
        QueueUrl=MAPPER_QUEUE_URL,
        Entries=[
            {'Id': str(task['shardId']), 'MessageBody': json.dumps(task)}
            for task in tasks
        ],
    )
    if response.get('Failed'):
        raise RuntimeError(
            f'Failed to publish Mapper tasks: {json.dumps(response["Failed"])}'
        )
    if len(response.get('Successful', [])) != NUM_SHARDS:
        raise RuntimeError('Mapper fan-out did not publish all five tasks')

    print(json.dumps({
        'event': 'guide_batch_dispatched',
        'jobId': guides[0]['JobID'],
        'targetIds': [int(guide['TargetID']) for guide in guides],
        'genome': genome,
        'batchIds': [task['batchId'] for task in tasks],
    }))


def lambda_handler(event, context):
    groups = {}
    for record in event.get('Records', []):
        guide, genome = _parse_guide(record)
        key = (str(guide['JobID']), genome)
        groups.setdefault(key, []).append(guide)

    for (_, genome), guides in groups.items():
        _dispatch(guides, genome)

    return {
        'processedGuides': len(event.get('Records', [])),
        'dispatchedBatches': len(groups),
    }
