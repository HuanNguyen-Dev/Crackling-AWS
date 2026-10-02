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
MAPPER_RESULTS = {
    32: struct.Struct('<QI4xdd'),
    64: struct.Struct('<QQdd'),
}
SCORE_RESULTS = {
    32: struct.Struct('<Id'),
    64: struct.Struct('<Qd'),
}

s3_client = boto3.client('s3')


def _parse_task(record):
    task = json.loads(record['body'])

    if task.get('taskType') == 'mapperReference':
        if task.get('schemaVersion') != 1:
            raise ValueError('Unsupported or missing Mapper task schemaVersion')

        # Retrieved task from S3
        batch = _read_json(task['bucket'], task['batchKey'])
        index = task['mapperTaskIndex']
        tasks = batch['mapperTasks']

        if type(index) is not int or not 0 <= index < len(tasks):
            raise ValueError("Invalid mapper task input")

        task = tasks[index]

    if (task.get('schemaVersion')) != 6:
        raise ValueError("Unsupported or missing Mapper task schemaVersion")

    required = (
        'batchId', 'jobId', 'guides', 'genome',
        'shardId', 'shardCount', 'sliceId', 'sliceIds', 'buckets',
        'idBits', 'hydratedRecordBytes', 'scoring', 'output',
    )
    for field in required:
        if field not in task:
            raise ValueError(f'Mapper task is missing {field}')

    if not task['guides']:
        raise ValueError('Mapper task must contain at least one guide')

    try:
        id_bits = int(task['idBits'])
        record_bytes = int(task['hydratedRecordBytes'])
        slice_ids = [int(value) for value in task['sliceIds']]
    except (TypeError, ValueError) as error:
        raise ValueError('Mapper task has invalid width or slice metadata') from error
    if id_bits not in MAPPER_RESULTS or record_bytes != 12 + id_bits // 8:
        raise ValueError('Mapper task hydrated-record width does not match ID width')
    if (len(slice_ids) != int(task['shardCount']) or not slice_ids
            or len(set(slice_ids)) != len(slice_ids)
            or int(task['sliceId']) not in slice_ids
            or int(task['shardId']) != int(task['sliceId'])):
        raise ValueError('Mapper task has invalid slice set')
    task['idBits'] = id_bits
    task['hydratedRecordBytes'] = record_bytes
    task['sliceIds'] = slice_ids

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


def _write_mapper_inputs(task, guides, bucket_plans, directory):
    query_path = os.path.join(directory, 'query.txt')
    shard_path = os.path.join(directory, 'shard.txt')
    with open(query_path, 'w', newline='\n') as query_file:
        query_file.write(
            ''.join(f'{guide["guideSequence"]}\n' for guide in guides)
        )
    with open(shard_path, 'w', newline='\n') as shard_file:
        shard_file.write(f'{task["shardId"]} {task["sliceId"]} {len(bucket_plans)}\n')
        for plan in bucket_plans:
            shard_file.write(
                f'{plan["bucketId"]} {plan["compactOffset"]} '
                f'{plan["elementCount"]}\n'
            )
        shard_file.write(f'{len(guides)}\n')
        for guide in guides:
            shard_file.write(f'{guide["bucketId"]}\n')
    return query_path, shard_path


def _sequence_to_signature(sequence):
    nucleotide_index = {'A': 0, 'C': 1, 'G': 2, 'T': 3}
    signature = 0
    for position, nucleotide in enumerate(sequence):
        signature |= nucleotide_index[nucleotide] << (position * 2)
    return signature


def _split_results(combined_path, guides, directory, id_bits):
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

    with open(combined_path, 'rb') as combined:
        active_signature = None
        outputs = ExitStack()
        mit_output = cfd_output = None
        mapper_result = MAPPER_RESULTS[id_bits]
        score_result = SCORE_RESULTS[id_bits]
        missing_id = (1 << id_bits) - 1
        try:
            while True:
                record = combined.read(mapper_result.size)
                if not record:
                    break
                if len(record) != mapper_result.size:
                    raise ValueError('Mapper produced a truncated binary record')
                query_signature, offtarget_id, mit_score, cfd_score = (
                    mapper_result.unpack(record)
                )
                if query_signature not in signatures:
                    raise ValueError(
                        f'Mapper returned unknown query signature {query_signature}'
                    )
                if query_signature != active_signature:
                    outputs.close()
                    outputs = ExitStack()
                    primary = results[signatures[query_signature][0]]
                    mit_output = outputs.enter_context(open(primary['mitPath'], 'ab'))
                    cfd_output = outputs.enter_context(open(primary['cfdPath'], 'ab'))
                    active_signature = query_signature
                for target_id in signatures[query_signature]:
                    results[target_id]['mapperRecords'] += 1
                if offtarget_id == missing_id:
                    continue
                if mit_score != 0.0:
                    mit_output.write(score_result.pack(offtarget_id, mit_score))
                    for target_id in signatures[query_signature]:
                        results[target_id]['mitRecords'] += 1
                if cfd_score != 0.0:
                    cfd_output.write(score_result.pack(offtarget_id, cfd_score))
                    for target_id in signatures[query_signature]:
                        results[target_id]['cfdRecords'] += 1
        finally:
            outputs.close()

    for target_ids in signatures.values():
        primary = results[target_ids[0]]
        for path in (primary['mitPath'], primary['cfdPath']):
            if not os.path.exists(path):
                with open(path, 'wb'):
                    pass
        for target_id in target_ids[1:]:
            result = results[target_id]
            shutil.copyfile(primary['mitPath'], result['mitPath'])
            shutil.copyfile(primary['cfdPath'], result['cfdPath'])
    return results


def _run_mapper(task, directory, query_path, shard_path):
    scoring = task.get('scoring', {})
    # The local C++ implementation prefixes its per-thread temporary names,
    # so this must remain a simple filename rather than an absolute path.
    output_prefix = 'result'
    command = [
        MAPPER_BINARY,
        os.path.join(directory, 'candidates.bin'),
        query_path,
        shard_path,
        str(task['idBits']),
        str(scoring.get('maxDistance', 4)),
        str(scoring.get('scoreThreshold', 75)),
        str(scoring.get('scoreMethod', 'and')),
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


def _read_json(bucket, key):
    return json.loads(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read())


def _download_object(bucket, key, output):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    while True:
        chunk = response['Body'].read(COPY_CHUNK_BYTES)
        if not chunk:
            break
        output.write(chunk)


def _materialize_candidates(task, directory):
    path = os.path.join(directory, 'candidates.bin')
    plans = []
    fragment_count = 0
    with open(path, 'wb') as output:
        for bucket in sorted(task['buckets'], key=lambda item: int(item['bucketId'])):
            manifest = _read_json(task['output']['bucket'], bucket['manifestKey'])
            if (manifest.get('schemaVersion') != 2
                    or int(manifest['sliceId']) != int(task['sliceId'])
                    or int(manifest['bucketId']) != int(bucket['bucketId'])):
                raise ValueError('Invalid hydrated bucket manifest')
            offset = output.tell()
            count = 0
            if manifest.get('recordFormat', {}).get('endianness') != 'little':
                raise ValueError('Hydrated bucket manifest must be little-endian')
            if manifest['recordFormat'].get('packing') != 'packed':
                raise ValueError('Hydrated bucket manifest must be packed')
            record_bytes = int(manifest['recordFormat']['recordBytes'])
            if (int(manifest['idBits']) != int(task['idBits'])
                    or record_bytes != int(task['hydratedRecordBytes'])
                    or record_bytes != 12 + int(task['idBits']) // 8):
                raise ValueError('Hydrated bucket width does not match Mapper task')

            for part in sorted(manifest['parts'], key=lambda item: int(item['startId'])):
                part_count = int(part['recordCount'])
                if part_count < 0:
                    raise ValueError('Hydrated candidate part has negative record count')
                if part_count:
                    if not part.get('key'):
                        raise ValueError('Non-empty hydrated candidate part has no key')
                    part_offset = output.tell()
                    _download_object(task['output']['bucket'], part['key'], output)
                    if output.tell() - part_offset != part_count * record_bytes:
                        raise ValueError('Hydrated candidate part does not match manifest')
                count += part_count
                fragment_count += 1

            if output.tell() - offset != count * record_bytes:
                raise ValueError('Hydrated candidate bytes do not match manifest')
            if count != int(manifest['recordCount']):
                raise ValueError('Hydrated candidate count does not match manifest')

            plans.append({'bucketId': int(bucket['bucketId']),
                          'compactOffset': offset, 'elementCount': count})
    return path, plans, fragment_count


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

    if not os.path.exists(MAPPER_BINARY):
        shutil.copyfile(MAPPER_BINARY_SOURCE, MAPPER_BINARY)
        os.chmod(MAPPER_BINARY, 0o755)

    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        _, bucket_plans, fragment_count = _materialize_candidates(task, directory)
        bucket_bytes = sum(
            plan['elementCount'] * task['hydratedRecordBytes']
            for plan in bucket_plans
        )
        query_path, shard_path = _write_mapper_inputs(
            task, guides, bucket_plans, directory,
        )
        combined_path = _run_mapper(
            task, directory, query_path, shard_path,
        )
        results = _split_results(
            combined_path, guides, directory, task['idBits'],
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
                    'fields': [f'targetId:uint{task["idBits"]}', 'score:float64'],
                    'recordBytes': SCORE_RESULTS[task['idBits']].size,
                },
                'records': {
                    'mapper': result['mapperRecords'],
                    'mit': result['mitRecords'],
                    'cfd': result['cfdRecords'],
                },
                'materializedBytes': {
                    'selectedBuckets': bucket_bytes,
                },
                'idBits': task['idBits'],
                'sliceIds': task['sliceIds'],
                'selectedBucketIds': [plan['bucketId'] for plan in bucket_plans],
                'fragmentCount': fragment_count,
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
