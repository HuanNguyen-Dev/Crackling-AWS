"""Execute idempotent map/reduce tasks for parallel ISSL creation."""

import gzip
import hashlib
import heapq
import json
import os
import re
import shutil
import struct
import subprocess
import tempfile
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


BUCKET = os.environ['BUCKET']
BUILD_TABLE_NAME = os.environ['BUILD_TABLE']
WORK_QUEUE = os.environ['WORK_QUEUE']
FINALIZE_QUEUE = os.environ['FINALIZE_QUEUE']
ISSL_CREATE_BIN = os.getenv('ISSL_CREATE_BIN', '/opt/ISSL/isslCreateIndex')
MERGE_FAN_IN = int(os.getenv('MERGE_FAN_IN', '4'))
SORT_CHUNK_LINES = int(os.getenv('SORT_CHUNK_LINES', '250000'))
SEQUENCE_LENGTH = 20
SLICE_WIDTH = 8
SLICE_COUNT = 5

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
build_table = boto3.resource('dynamodb').Table(BUILD_TABLE_NAME)

FORWARD = re.compile(r'(?=([ACG][ACGT]{19}[ACGT][AG]G))')
REVERSE = re.compile(r'(?=(C[CT][ACGT][ACGT]{19}[TGC]))')
COMPLEMENT = str.maketrans(
    'acgtrymkbdhvACGTRYMKBDHV',
    'tgcayrkmvhdbTGCAYRKMVHDB',
)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _send(queue_url, body):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


def _reverse_complement(sequence):
    return sequence.translate(COMPLEMENT)[::-1]


def extract_offtargets_from_records(lines):
    """Yield all targets while retaining only the 22-base boundary overlap."""
    overlap = ''
    for raw_line in lines:
        line = raw_line.strip().upper()
        if not line:
            continue
        if line.startswith('>'):
            overlap = ''
            continue
        sequence = overlap + line
        for match in FORWARD.finditer(sequence):
            yield match.group(1)[:SEQUENCE_LENGTH]
        for match in REVERSE.finditer(sequence):
            yield _reverse_complement(match.group(1))[:SEQUENCE_LENGTH]
        overlap = sequence[-22:]


def _flush_sorted_chunk(values, directory, index):
    values.sort()
    path = os.path.join(directory, f'chunk-{index:06d}.txt')
    with open(path, 'w', newline='\n') as handle:
        handle.writelines(f'{value}\n' for value in values)
    return path


def _merge_local_files(input_paths, output_path):
    handles = [open(path, 'r') for path in input_paths]
    try:
        with open(output_path, 'w', newline='\n') as output:
            output.writelines(heapq.merge(*handles))
    finally:
        for handle in handles:
            handle.close()


def _extract(message, temp_dir):
    source_path = os.path.join(temp_dir, 'source')
    s3_client.download_file(
        message['source']['bucket'], message['source']['key'], source_path
    )
    opener = gzip.open if message['source']['key'].lower().endswith('.gz') else open
    chunks = []
    values = []
    with opener(source_path, 'rt', encoding='ascii', errors='strict') as lines:
        for target in extract_offtargets_from_records(lines):
            values.append(target)
            if len(values) >= SORT_CHUNK_LINES:
                chunks.append(_flush_sorted_chunk(values, temp_dir, len(chunks)))
                values = []
    if values or not chunks:
        chunks.append(_flush_sorted_chunk(values, temp_dir, len(chunks)))

    output_path = os.path.join(temp_dir, 'extract.run')
    if len(chunks) == 1:
        shutil.move(chunks[0], output_path)
    else:
        _merge_local_files(chunks, output_path)
    s3_client.upload_file(output_path, BUCKET, message['outputKey'])
    return {'outputKey': message['outputKey']}


def _merge(message, temp_dir):
    paths = []
    for index, key in enumerate(message['inputKeys']):
        path = os.path.join(temp_dir, f'input-{index:04d}.run')
        s3_client.download_file(BUCKET, key, path)
        paths.append(path)
    output_path = os.path.join(temp_dir, 'merged.run')
    _merge_local_files(paths, output_path)
    s3_client.upload_file(output_path, BUCKET, message['outputKey'])
    return {'outputKey': message['outputKey']}


def _catalog(message, temp_dir):
    input_path = os.path.join(temp_dir, 'sites.run')
    output_path = os.path.join(temp_dir, 'sites.catalog')
    s3_client.download_file(BUCKET, message['inputKey'], input_path)
    subprocess.run(
        [ISSL_CREATE_BIN, 'catalog', input_path, str(SEQUENCE_LENGTH), output_path],
        check=True,
    )
    with open(output_path, 'rb') as handle:
        magic = handle.read(8)
        sequence_length, sequence_count, distinct_count = struct.unpack('<3Q', handle.read(24))
    if magic != b'ISSLCAT1' or sequence_length != SEQUENCE_LENGTH:
        raise ValueError('Creator produced an invalid catalogue header')
    s3_client.upload_file(output_path, BUCKET, message['outputKey'])
    return {
        'outputKey': message['outputKey'],
        'sequenceCount': sequence_count,
        'distinctCount': distinct_count,
    }


def _slice(message, temp_dir):
    input_path = os.path.join(temp_dir, 'sites.catalog')
    output_path = os.path.join(temp_dir, f'slice-{message["sliceId"]}.bin')
    s3_client.download_file(BUCKET, message['inputKey'], input_path)
    subprocess.run([
        ISSL_CREATE_BIN,
        'slice',
        input_path,
        str(SLICE_WIDTH),
        str(message['sliceId']),
        output_path,
    ], check=True)
    with open(output_path, 'rb') as handle:
        if handle.read(8) != b'ISSLSLC1':
            raise ValueError('Creator produced an invalid slice fragment')
    s3_client.upload_file(output_path, BUCKET, message['outputKey'])
    return {'outputKey': message['outputKey'], 'sliceId': message['sliceId']}


def _task_record_id(message):
    return f'TASK#{message["buildId"]}#{message["stage"]}#{message["taskId"]}'


def _mark_complete(message, result):
    item = {
        'Genome': message['genome'],
        'RecordId': _task_record_id(message),
        'buildId': message['buildId'],
        'stage': message['stage'],
        'taskId': message['taskId'],
        'status': 'DONE',
        'result': result,
        'completedAt': _now(),
    }
    try:
        build_table.put_item(Item=item, ConditionExpression='attribute_not_exists(RecordId)')
    except ClientError as error:
        if error.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return None
        raise
    response = build_table.update_item(
        Key={
            'Genome': message['genome'],
            'RecordId': f'BUILD#{message["buildId"]}',
        },
        UpdateExpression='ADD completedTasks :one SET updatedAt = :now',
        ExpressionAttributeValues={':one': 1, ':now': _now()},
        ReturnValues='ALL_NEW',
    )
    return response['Attributes']


def _stage_results(genome, build_id, stage):
    expression = (
        Key('Genome').eq(genome)
        & Key('RecordId').begins_with(f'TASK#{build_id}#{stage}#')
    )
    items = []
    start_key = None
    while True:
        arguments = {
            'KeyConditionExpression': expression,
            'ConsistentRead': True,
        }
        if start_key:
            arguments['ExclusiveStartKey'] = start_key
        response = build_table.query(**arguments)
        items.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey')
        if not start_key:
            break
    return sorted(items, key=lambda item: item['taskId'])


def _start_stage(build, old_stage, new_stage, messages):
    if not messages:
        raise ValueError(f'Cannot start empty stage {new_stage}')
    try:
        build_table.update_item(
            Key={'Genome': build['Genome'], 'RecordId': f'BUILD#{build["buildId"]}'},
            UpdateExpression=(
                'SET phase = :new, #status = :status, expectedTasks = :expected, '
                'completedTasks = :zero, updatedAt = :now'
            ),
            ConditionExpression='phase = :old AND completedTasks = expectedTasks',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':old': old_stage,
                ':new': new_stage,
                ':status': new_stage.upper(),
                ':expected': len(messages),
                ':zero': 0,
                ':now': _now(),
            },
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return False
        raise
    for message in messages:
        _send(WORK_QUEUE, message)
    return True


def _base_task(build, stage, task_id, task_type):
    return {
        'schemaVersion': 1,
        'taskType': task_type,
        'stage': stage,
        'genome': build['Genome'],
        'buildId': build['buildId'],
        'taskId': task_id,
    }


def _schedule_after_stage(build):
    genome = build['Genome']
    build_id = build['buildId']
    stage = build['phase']
    results = _stage_results(genome, build_id, stage)
    keys = [item['result']['outputKey'] for item in results]

    if stage == 'extract' or stage.startswith('merge-'):
        if len(keys) > 1:
            round_number = 1 if stage == 'extract' else int(stage.split('-')[1]) + 1
            next_stage = f'merge-{round_number}'
            messages = []
            for group_index in range(0, len(keys), MERGE_FAN_IN):
                task_id = f'merge-{round_number}-{group_index // MERGE_FAN_IN:06d}'
                message = _base_task(build, next_stage, task_id, 'merge')
                message.update({
                    'inputKeys': keys[group_index:group_index + MERGE_FAN_IN],
                    'outputKey': (
                        f'{genome}/issl-builds/{build_id}/merge/'
                        f'{round_number}/{group_index // MERGE_FAN_IN:06d}.run'
                    ),
                })
                messages.append(message)
            _start_stage(build, stage, next_stage, messages)
            return

        catalog_stage = 'catalog'
        message = _base_task(build, catalog_stage, 'catalog-000000', 'catalog')
        message.update({
            'inputKey': keys[0],
            'outputKey': f'{genome}/issl-builds/{build_id}/catalog/sites.catalog',
        })
        _start_stage(build, stage, catalog_stage, [message])
        return

    if stage == 'catalog':
        catalog = results[0]['result']
        messages = []
        for slice_id in range(SLICE_COUNT):
            message = _base_task(build, 'slice', f'slice-{slice_id}', 'slice')
            message.update({
                'sliceId': slice_id,
                'inputKey': catalog['outputKey'],
                'outputKey': f'{genome}/issl-builds/{build_id}/slices/{slice_id}.slice',
            })
            messages.append(message)
        _start_stage(build, stage, 'slice', messages)
        return

    if stage == 'slice':
        try:
            build_table.update_item(
                Key={'Genome': genome, 'RecordId': f'BUILD#{build_id}'},
                UpdateExpression='SET phase = :phase, #status = :status, updatedAt = :now',
                ConditionExpression='phase = :old AND completedTasks = expectedTasks',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':old': 'slice', ':phase': 'finalize', ':status': 'FINALIZING', ':now': _now()
                },
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return
            raise
        catalog = _stage_results(genome, build_id, 'catalog')[0]['result']
        _send(FINALIZE_QUEUE, {
            'schemaVersion': 1,
            'genome': genome,
            'buildId': build_id,
            'catalogKey': catalog['outputKey'],
            'sequenceCount': int(catalog['sequenceCount']),
            'distinctCount': int(catalog['distinctCount']),
            'sliceKeys': keys,
            'finalKey': build['finalKey'],
        })


def _process(message):
    if message.get('schemaVersion') != 1:
        raise ValueError('Unsupported work-message schema')
    temp_dir = tempfile.mkdtemp(prefix='issl-')
    try:
        handlers = {
            'extract': _extract,
            'merge': _merge,
            'catalog': _catalog,
            'slice': _slice,
        }
        result = handlers[message['taskType']](message, temp_dir)
        build = _mark_complete(message, result)
        if build and int(build['completedTasks']) == int(build['expectedTasks']):
            _schedule_after_stage(build)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def lambda_handler(event, context):
    failures = []
    for record in event.get('Records', []):
        try:
            _process(json.loads(record['body']))
        except Exception:
            failures.append({'itemIdentifier': record.get('messageId', 'unknown')})
            print(json.dumps({'event': 'issl_worker_failed', 'record': record}, default=str))
    return {'batchItemFailures': failures}
