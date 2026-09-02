import json
import os
import shutil
import struct
import subprocess
import tempfile
from contextlib import ExitStack

import boto3
from botocore.exceptions import ClientError


MAPPER_BINARY_SOURCE = '/opt/mapper'
MAPPER_BINARY = '/tmp/mapper'
COPY_CHUNK_BYTES = 8 * 1024 * 1024
RANGE_MERGE_GAP_BYTES = 8 * 1024 * 1024
UINT64_BYTES = 8
MAPPER_RESULT = struct.Struct('<QI4xdd')
SCORE_RESULT = struct.Struct('<Id')

s3_client = boto3.client('s3')


def _parse_task(record):
    task = json.loads(record['body'])
    if task.get('schemaVersion') != 4:
        raise ValueError('Unsupported or missing Mapper task schemaVersion')

    required = (
        'batchId', 'jobId', 'guides', 'genome',
        'shardId', 'shardCount', 'sliceId', 'selectedBuckets', 'ranges',
        'issl', 'manifest', 'output',
    )
    for field in required:
        if field not in task:
            raise ValueError(f'Mapper task is missing {field}')

    if not task['guides']:
        raise ValueError('Mapper task must contain at least one guide')
    if len(task['guides']) > 5:
        raise ValueError('Mapper task cannot contain more than five guides')

    target_ids = set()
    for guide in task['guides']:
        for field in ('taskId', 'targetId', 'guideSequence', 'bucketId', 'output'):
            if field not in guide:
                raise ValueError(f'Mapper guide is missing {field}')
        for field in ('mitKey', 'cfdKey', 'metadataKey'):
            if field not in guide['output']:
                raise ValueError(f'Mapper guide output is missing {field}')
        sequence = guide['guideSequence'][:20].upper()
        if len(sequence) != 20 or any(base not in 'ACGT' for base in sequence):
            raise ValueError(
                'Each Mapper guide must begin with exactly 20 A/C/G/T bases'
            )
        target_id = int(guide['targetId'])
        if target_id in target_ids:
            raise ValueError(f'Duplicate target ID in Mapper batch: {target_id}')
        target_ids.add(target_id)
        guide['targetId'] = target_id
        guide['bucketId'] = int(guide['bucketId'])
        guide['guideSequence'] = sequence
    return task


def _already_complete(task, guide):
    output = guide['output']
    try:
        response = s3_client.get_object(
            Bucket=task['output']['bucket'],
            Key=output['metadataKey'],
        )
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return False
        raise

    metadata = json.loads(response['Body'].read())
    return metadata.get('taskId') == guide['taskId']


def _copy_s3_range(bucket, key, start, end, destination, offset):
    if end <= start:
        return 0
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
        Range=f'bytes={start}-{end - 1}',
    )
    copied = 0
    destination.seek(offset)
    body = response['Body']
    while True:
        chunk = body.read(COPY_CHUNK_BYTES)
        if not chunk:
            break
        destination.write(chunk)
        copied += len(chunk)
    if copied != end - start:
        raise ValueError(
            f'Expected {end - start} bytes from s3://{bucket}/{key}, '
            f'received {copied}'
        )
    return copied


def _merge_bucket_ranges(buckets):
    nonempty = [item for item in buckets if item['endByte'] > item['startByte']]
    ranges = []
    for bucket in sorted(nonempty, key=lambda item: item['startByte']):
        if ranges and bucket['startByte'] - ranges[-1]['endByte'] <= RANGE_MERGE_GAP_BYTES:
            ranges[-1]['endByte'] = max(ranges[-1]['endByte'], bucket['endByte'])
        else:
            ranges.append({
                'startByte': bucket['startByte'],
                'endByte': bucket['endByte'],
            })
    return ranges


def _bucket_plan(task, manifest, guides):
    if manifest.get('schemaVersion') != 2:
        raise ValueError('Unsupported or missing shard manifest schemaVersion')
    shards = manifest.get('shards', [])
    shard_id = int(task['shardId'])
    try:
        shard = next(item for item in shards if int(item['shardId']) == shard_id)
    except StopIteration as error:
        raise ValueError(f'Manifest is missing shard {shard_id}') from error
    if int(shard['sliceId']) != int(task['sliceId']):
        raise ValueError('Mapper task slice does not match manifest')

    offsets = [int(value) for value in shard['bucketOffsets']]
    slice_limit = int(manifest['layout']['sliceLimit'])
    if len(offsets) != slice_limit + 1:
        raise ValueError('Manifest has invalid bucket offsets')
    if any(end < start for start, end in zip(offsets, offsets[1:])):
        raise ValueError('Manifest bucket offsets are not monotonic')

    bucket_ids = sorted({int(guide['bucketId']) for guide in guides})
    if any(bucket_id < 0 or bucket_id >= slice_limit for bucket_id in bucket_ids):
        raise ValueError('Mapper guide bucket ID is outside the slice')
    slice_width = int(manifest['layout']['sliceWidth'])
    for guide in guides:
        signature = _sequence_to_signature(guide['guideSequence'])
        expected = (
            signature >> (int(task['sliceId']) * slice_width)
        ) & (slice_limit - 1)
        if int(guide['bucketId']) != expected:
            raise ValueError('Mapper guide bucket ID does not match its sequence')
    buckets = [
        {
            'bucketId': bucket_id,
            'startByte': offsets[bucket_id],
            'endByte': offsets[bucket_id + 1],
        }
        for bucket_id in bucket_ids
    ]
    ranges = _merge_bucket_ranges(buckets)
    return buckets, ranges


def _validate_dispatcher_plan(task, manifest):
    buckets, ranges = _bucket_plan(task, manifest, task['guides'])
    expected_buckets = [
        {
            'bucketId': int(item['bucketId']),
            'startByte': int(item['startByte']),
            'endByte': int(item['endByte']),
        }
        for item in task['selectedBuckets']
    ]
    expected_ranges = [
        {
            'startByte': int(item['startByte']),
            'endByte': int(item['endByte']),
            'bucketIds': [int(value) for value in item['bucketIds']],
        }
        for item in task['ranges']
    ]
    calculated_ranges = _merge_bucket_ranges(buckets)
    for item in calculated_ranges:
        item['bucketIds'] = [
            bucket['bucketId'] for bucket in buckets
            if item['startByte'] <= bucket['startByte']
            and bucket['endByte'] <= item['endByte']
            and bucket['endByte'] > bucket['startByte']
        ]
    if expected_buckets != buckets or expected_ranges != calculated_ranges:
        raise ValueError('Mapper task bucket plan does not match its manifest')


def _materialize_compact_issl(task, manifest, guides, path):
    source = task['issl']
    base_offset = int(manifest['layout']['baseOffsetBytes'])
    buckets, ranges = _bucket_plan(task, manifest, guides)
    if not 0 < base_offset:
        raise ValueError('Invalid ISSL prefix boundary')

    with open(path, 'w+b') as destination:
        prefix_bytes = _copy_s3_range(
            source['bucket'], source['key'], 0, base_offset,
            destination, 0,
        )
        compact_offset = base_offset
        compact_ranges = []
        for item in ranges:
            copied = _copy_s3_range(
                source['bucket'], source['key'], item['startByte'],
                item['endByte'], destination, compact_offset,
            )
            compact_ranges.append({**item, 'compactOffset': compact_offset})
            compact_offset += copied
        destination.truncate(compact_offset)

    plans = []
    for bucket in buckets:
        if bucket['endByte'] == bucket['startByte']:
            bucket_compact_offset = base_offset
        else:
            containing = next(
                item for item in compact_ranges
                if item['startByte'] <= bucket['startByte']
                and bucket['endByte'] <= item['endByte']
            )
            bucket_compact_offset = (
                containing['compactOffset']
                + bucket['startByte'] - containing['startByte']
            )
        plans.append({
            'bucketId': bucket['bucketId'],
            'compactOffset': bucket_compact_offset,
            'elementCount': (
                bucket['endByte'] - bucket['startByte']
            ) // UINT64_BYTES,
        })
    return prefix_bytes, compact_offset - base_offset, plans, compact_ranges


def _write_mapper_inputs(task, guides, bucket_plans, directory):
    query_path = os.path.join(directory, 'query.txt')
    shard_path = os.path.join(directory, 'shard.txt')
    with open(query_path, 'w', newline='\n') as query_file:
        query_file.write(
            ''.join(f'{guide["guideSequence"]}\n' for guide in guides)
        )
    with open(shard_path, 'w', newline='\n') as shard_file:
        shard_file.write(
            f'{task["shardId"]} {task["sliceId"]} {len(bucket_plans)}\n'
        )
        for plan in bucket_plans:
            shard_file.write(
                f'{plan["bucketId"]} {plan["compactOffset"]} '
                f'{plan["elementCount"]}\n'
            )
    return query_path, shard_path


def _sequence_to_signature(sequence):
    nucleotide_index = {'A': 0, 'C': 1, 'G': 2, 'T': 3}
    signature = 0
    for position, nucleotide in enumerate(sequence):
        signature |= nucleotide_index[nucleotide] << (position * 2)
    return signature


def _split_results(combined_path, guides, directory):
    results = {}
    signatures = {}
    for guide in guides:
        signature = _sequence_to_signature(guide['guideSequence'])
        signatures.setdefault(signature, []).append(guide['targetId'])
        results[guide['targetId']] = {
            'mapperRecords': 0,
            'mitRecords': 0,
            'cfdRecords': 0,
            'mitPath': os.path.join(directory, f'{guide["targetId"]}-mit.bin'),
            'cfdPath': os.path.join(directory, f'{guide["targetId"]}-cfd.bin'),
        }

    with ExitStack() as stack:
        combined = stack.enter_context(open(combined_path, 'rb'))
        outputs = {
            target_id: {
                'mit': stack.enter_context(open(result['mitPath'], 'wb')),
                'cfd': stack.enter_context(open(result['cfdPath'], 'wb')),
            }
            for target_id, result in results.items()
        }
        while True:
            record = combined.read(MAPPER_RESULT.size)
            if not record:
                break
            if len(record) != MAPPER_RESULT.size:
                raise ValueError('Mapper produced a truncated binary record')
            query_signature, offtarget_id, mit_score, cfd_score = (
                MAPPER_RESULT.unpack(record)
            )
            if query_signature not in signatures:
                raise ValueError(
                    f'Mapper returned unknown query signature {query_signature}'
                )
            for target_id in signatures[query_signature]:
                result = results[target_id]
                result['mapperRecords'] += 1
                if offtarget_id == 0xFFFFFFFF:
                    continue
                if mit_score != 0.0:
                    outputs[target_id]['mit'].write(
                        SCORE_RESULT.pack(offtarget_id, mit_score)
                    )
                    result['mitRecords'] += 1
                if cfd_score != 0.0:
                    outputs[target_id]['cfd'].write(
                        SCORE_RESULT.pack(offtarget_id, cfd_score)
                    )
                    result['cfdRecords'] += 1
    return results


def _run_mapper(task, directory, issl_path, query_path, shard_path):
    scoring = task.get('scoring', {})
    # The local C++ implementation prefixes its per-thread temporary names,
    # so this must remain a simple filename rather than an absolute path.
    output_prefix = 'result'
    command = [
        MAPPER_BINARY,
        issl_path,
        query_path,
        str(scoring.get('maxDistance', 4)),
        str(scoring.get('scoreThreshold', 75)),
        str(scoring.get('scoreMethod', 'and')),
        shard_path,
        output_prefix,
    ]
    completed = subprocess.run(
        command,
        cwd=directory,
        check=False,
        capture_output=True,
        text=True,
    )
    print(completed.stdout)
    if completed.returncode != 0:
        raise RuntimeError(
            f'Mapper exited with {completed.returncode}: {completed.stderr}'
        )
    return os.path.join(
        directory,
        f'{output_prefix}_shard_{task["shardId"]}.bin',
    )


def _upload_results(task, guide, result, metadata):
    output = guide['output']
    bucket = task['output']['bucket']
    extra_args = {'ContentType': 'application/octet-stream'}
    s3_client.upload_file(
        result['mitPath'], bucket, output['mitKey'], ExtraArgs=extra_args,
    )
    s3_client.upload_file(
        result['cfdPath'], bucket, output['cfdKey'], ExtraArgs=extra_args,
    )
    # Written last: this object is the durable completion marker.
    s3_client.put_object(
        Bucket=bucket,
        Key=output['metadataKey'],
        Body=json.dumps(metadata, separators=(',', ':')).encode('utf-8'),
        ContentType='application/json',
    )


def _process(task):
    guides = [
        guide for guide in task['guides']
        if not _already_complete(task, guide)
    ]
    if not guides:
        print(json.dumps({
            'event': 'mapper_batch_already_complete',
            'batchId': task['batchId'],
        }))
        return

    manifest_response = s3_client.get_object(
        Bucket=task['manifest']['bucket'],
        Key=task['manifest']['key'],
    )
    manifest = json.loads(manifest_response['Body'].read())
    _validate_dispatcher_plan(task, manifest)

    if not os.path.exists(MAPPER_BINARY):
        shutil.copyfile(MAPPER_BINARY_SOURCE, MAPPER_BINARY)
        os.chmod(MAPPER_BINARY, 0o755)

    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        issl_path = os.path.join(directory, 'index.issl')
        prefix_bytes, bucket_bytes, bucket_plans, compact_ranges = (
            _materialize_compact_issl(task, manifest, guides, issl_path)
        )
        query_path, shard_path = _write_mapper_inputs(
            task, guides, bucket_plans, directory,
        )
        combined_path = _run_mapper(
            task, directory, issl_path, query_path, shard_path,
        )
        results = _split_results(
            combined_path, guides, directory,
        )
        for guide in guides:
            result = results[guide['targetId']]
            metadata = {
                'schemaVersion': 1,
                'taskId': guide['taskId'],
                'batchId': task['batchId'],
                'jobId': task['jobId'],
                'targetId': guide['targetId'],
                'guideSequence': guide['guideSequence'],
                'genome': task['genome'],
                'shardId': task['shardId'],
                'shardCount': task['shardCount'],
                'recordFormat': {
                    'endianness': 'little',
                    'fields': ['targetId:uint32', 'score:float64'],
                    'recordBytes': SCORE_RESULT.size,
                },
                'records': {
                    'mapper': result['mapperRecords'],
                    'mit': result['mitRecords'],
                    'cfd': result['cfdRecords'],
                },
                'materializedBytes': {
                    'indexPrefix': prefix_bytes,
                    'selectedBuckets': bucket_bytes,
                },
                'selectedBucketIds': [plan['bucketId'] for plan in bucket_plans],
                'rangeCount': len(compact_ranges),
                'outputs': {
                    'mit': guide['output']['mitKey'],
                    'cfd': guide['output']['cfdKey'],
                },
            }
            _upload_results(task, guide, result, metadata)

    print(json.dumps({
        'event': 'mapper_batch_complete',
        'batchId': task['batchId'],
        'jobId': task['jobId'],
        'targetIds': [guide['targetId'] for guide in guides],
        'shardId': task['shardId'],
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process(_parse_task(record))
    return {'processed': len(event.get('Records', []))}
