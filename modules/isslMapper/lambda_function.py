import json
import os
import shutil
import struct
import subprocess
import tempfile

import boto3
from botocore.exceptions import ClientError


MAPPER_BINARY_SOURCE = '/opt/mapper'
MAPPER_BINARY = '/tmp/mapper'
COPY_CHUNK_BYTES = 8 * 1024 * 1024
MAPPER_RESULT = struct.Struct('<QI4xdd')
SCORE_RESULT = struct.Struct('<Id')

s3_client = boto3.client('s3')


def _parse_task(record):
    task = json.loads(record['body'])
    if task.get('schemaVersion') != 2:
        raise ValueError('Unsupported or missing Mapper task schemaVersion')

    required = (
        'taskId', 'jobId', 'targetId', 'guideSequence', 'genome',
        'shardId', 'shardCount', 'startSlice', 'endSlice',
        'startByte', 'endByte', 'issl', 'output',
    )
    for field in required:
        if field not in task:
            raise ValueError(f'Mapper task is missing {field}')

    guide = task['guideSequence'][:20].upper()
    if len(guide) != 20 or any(base not in 'ACGT' for base in guide):
        raise ValueError('Mapper guide must begin with exactly 20 A/C/G/T bases')
    task['guideSequence'] = guide
    return task


def _already_complete(task):
    output = task['output']
    try:
        response = s3_client.get_object(
            Bucket=output['bucket'],
            Key=output['metadataKey'],
        )
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return False
        raise

    metadata = json.loads(response['Body'].read())
    return metadata.get('taskId') == task['taskId']


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


def _materialize_sparse_issl(task, manifest, path):
    source = task['issl']
    base_offset = int(manifest['layout']['baseOffsetBytes'])
    start_byte = int(task['startByte'])
    end_byte = int(task['endByte'])
    if not 0 < base_offset <= start_byte <= end_byte:
        raise ValueError('Invalid ISSL shard byte boundaries')

    with open(path, 'w+b') as destination:
        prefix_bytes = _copy_s3_range(
            source['bucket'], source['key'], 0, base_offset,
            destination, 0,
        )
        shard_bytes = _copy_s3_range(
            source['bucket'], source['key'], start_byte, end_byte,
            destination, start_byte,
        )
        destination.truncate(end_byte)
    return prefix_bytes, shard_bytes


def _write_mapper_inputs(task, directory):
    query_path = os.path.join(directory, 'query.txt')
    shard_path = os.path.join(directory, 'shard.txt')
    with open(query_path, 'w', newline='\n') as query_file:
        query_file.write(f'{task["guideSequence"]}\n')
    with open(shard_path, 'w', newline='\n') as shard_file:
        shard_file.write(
            f'{task["shardId"]} {task["startSlice"]} '
            f'{task["endSlice"]} {task["startByte"]} '
            f'{task["endByte"]}\n'
        )
    return query_path, shard_path


def _split_results(combined_path, mit_path, cfd_path):
    input_records = 0
    mit_records = 0
    cfd_records = 0
    with (
        open(combined_path, 'rb') as combined,
        open(mit_path, 'wb') as mit_output,
        open(cfd_path, 'wb') as cfd_output,
    ):
        while True:
            record = combined.read(MAPPER_RESULT.size)
            if not record:
                break
            if len(record) != MAPPER_RESULT.size:
                raise ValueError('Mapper produced a truncated binary record')
            _, target_id, mit_score, cfd_score = MAPPER_RESULT.unpack(record)
            input_records += 1
            if target_id == 0xFFFFFFFF:
                continue
            if mit_score != 0.0:
                mit_output.write(SCORE_RESULT.pack(target_id, mit_score))
                mit_records += 1
            if cfd_score != 0.0:
                cfd_output.write(SCORE_RESULT.pack(target_id, cfd_score))
                cfd_records += 1
    return input_records, mit_records, cfd_records


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


def _upload_results(task, mit_path, cfd_path, metadata):
    output = task['output']
    extra_args = {'ContentType': 'application/octet-stream'}
    s3_client.upload_file(
        mit_path, output['bucket'], output['mitKey'], ExtraArgs=extra_args,
    )
    s3_client.upload_file(
        cfd_path, output['bucket'], output['cfdKey'], ExtraArgs=extra_args,
    )
    # Written last: this object is the durable completion marker.
    s3_client.put_object(
        Bucket=output['bucket'],
        Key=output['metadataKey'],
        Body=json.dumps(metadata, separators=(',', ':')).encode('utf-8'),
        ContentType='application/json',
    )


def _process(task):
    if _already_complete(task):
        print(json.dumps({'event': 'mapper_task_already_complete', 'taskId': task['taskId']}))
        return

    manifest_response = s3_client.get_object(
        Bucket=task['manifest']['bucket'],
        Key=task['manifest']['key'],
    )
    manifest = json.loads(manifest_response['Body'].read())

    if not os.path.exists(MAPPER_BINARY):
        shutil.copyfile(MAPPER_BINARY_SOURCE, MAPPER_BINARY)
        os.chmod(MAPPER_BINARY, 0o755)

    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        issl_path = os.path.join(directory, 'index.issl')
        prefix_bytes, shard_bytes = _materialize_sparse_issl(
            task, manifest, issl_path,
        )
        query_path, shard_path = _write_mapper_inputs(task, directory)
        combined_path = _run_mapper(
            task, directory, issl_path, query_path, shard_path,
        )
        mit_path = os.path.join(directory, 'mit.bin')
        cfd_path = os.path.join(directory, 'cfd.bin')
        input_records, mit_records, cfd_records = _split_results(
            combined_path, mit_path, cfd_path,
        )
        metadata = {
            'schemaVersion': 1,
            'taskId': task['taskId'],
            'jobId': task['jobId'],
            'targetId': task['targetId'],
            'guideSequence': task['guideSequence'],
            'genome': task['genome'],
            'shardId': task['shardId'],
            'shardCount': task['shardCount'],
            'recordFormat': {
                'endianness': 'little',
                'fields': ['targetId:uint32', 'score:float64'],
                'recordBytes': SCORE_RESULT.size,
            },
            'records': {
                'mapper': input_records,
                'mit': mit_records,
                'cfd': cfd_records,
            },
            'materializedBytes': {
                'indexPrefix': prefix_bytes,
                'shard': shard_bytes,
            },
            'outputs': {
                'mit': task['output']['mitKey'],
                'cfd': task['output']['cfdKey'],
            },
        }
        _upload_results(task, mit_path, cfd_path, metadata)

    print(json.dumps({
        'event': 'mapper_task_complete',
        'taskId': task['taskId'],
        'jobId': task['jobId'],
        'targetId': task['targetId'],
        'shardId': task['shardId'],
        'mitRecords': mit_records,
        'cfdRecords': cfd_records,
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process(_parse_task(record))
    return {'processed': len(event.get('Records', []))}
