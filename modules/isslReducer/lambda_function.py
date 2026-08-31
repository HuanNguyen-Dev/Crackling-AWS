import json
import math
import os
import random
import re
import sqlite3
import struct
import tempfile
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


SHARD_COUNT = int(os.getenv('SHARD_COUNT', '5'))
TRANSACTION_MAX_ATTEMPTS = 5
TRANSACTION_BACKOFF_BASE_SECONDS = 0.05
TRANSACTION_BACKOFF_MAX_SECONDS = 1.0
TARGETS_TABLE = os.getenv('TARGETS_TABLE')
TASK_TRACKING_TABLE = os.getenv('TASK_TRACKING_TABLE')
SCORE_RECORD = struct.Struct('<Id')
MAPPER_MARKER_PATTERN = re.compile(
    r'^(?P<genome>.+)/mapper/(?P<job_id>[^/]+)/targets/'
    r'(?P<target_id>\d+)/shards/(?P<shard_id>\d+)/result\.json$'
)

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')


def _mapper_event(record):
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    match = MAPPER_MARKER_PATTERN.fullmatch(key)
    if not match:
        return None
    values = match.groupdict()
    return {
        'bucket': bucket,
        'key': key,
        'genome': values['genome'],
        'jobId': values['job_id'],
        'targetId': int(values['target_id']),
        'shardId': int(values['shard_id']),
    }


def _s3_records(record):
    """Return S3 event records from either direct S3 or SQS delivery."""
    if 's3' in record:
        return [record]
    if record.get('eventSource') != 'aws:sqs':
        return []
    message = json.loads(record['body'])
    return message.get('Records', [])


def _get_json(bucket, key, missing_is_none=False):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        code = error.response.get('Error', {}).get('Code')
        if missing_is_none and code in ('404', 'NoSuchKey'):
            return None
        raise
    return json.loads(response['Body'].read())


def _marker_key(trigger, shard_id):
    return (
        f'{trigger["genome"]}/mapper/{trigger["jobId"]}/targets/'
        f'{trigger["targetId"]}/shards/{shard_id}/result.json'
    )


def _load_all_markers(trigger):
    markers = []
    for shard_id in range(SHARD_COUNT):
        marker = _get_json(
            trigger['bucket'],
            _marker_key(trigger, shard_id),
            missing_is_none=True,
        )
        if marker is None:
            return None
        expected = {
            'jobId': trigger['jobId'],
            'targetId': trigger['targetId'],
            'genome': trigger['genome'],
            'shardId': shard_id,
            'shardCount': SHARD_COUNT,
        }
        for field, value in expected.items():
            if marker.get(field) != value:
                raise ValueError(
                    f'Mapper marker {shard_id} has invalid {field}: '
                    f'{marker.get(field)!r}'
                )
        markers.append(marker)
    return markers


def _create_score_table(connection, score_name):
    connection.execute(
        f'CREATE TABLE {score_name} ('
        'target_id INTEGER PRIMARY KEY, score REAL NOT NULL)'
    )


def _read_score_records(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    remainder = b''
    while True:
        chunk = body.read(8 * 1024 * 1024)
        if not chunk:
            break
        data = remainder + chunk
        complete_bytes = len(data) - (len(data) % SCORE_RECORD.size)
        for offset in range(0, complete_bytes, SCORE_RECORD.size):
            yield SCORE_RECORD.unpack_from(data, offset)
        remainder = data[complete_bytes:]
    if remainder:
        raise ValueError(f'Score object {key} contains a truncated record')


def _merge_score_object(connection, score_name, bucket, key):
    unique_count = 0
    duplicate_count = 0
    for target_id, score in _read_score_records(bucket, key):
        cursor = connection.execute(
            f'INSERT OR IGNORE INTO {score_name} (target_id, score) '
            'VALUES (?, ?)',
            (target_id, score),
        )
        if cursor.rowcount == 1:
            unique_count += 1
            continue

        duplicate_count += 1
        existing = connection.execute(
            f'SELECT score FROM {score_name} WHERE target_id = ?',
            (target_id,),
        ).fetchone()[0]
        if not math.isclose(existing, score, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError(
                f'Conflicting {score_name.upper()} contribution for '
                f'off-target {target_id}: {existing} != {score}'
            )
    connection.commit()
    return unique_count, duplicate_count


def _reduce_markers(bucket, markers, database_path):
    connection = sqlite3.connect(database_path)
    try:
        _create_score_table(connection, 'mit')
        _create_score_table(connection, 'cfd')
        source_records = {'mit': 0, 'cfd': 0}
        duplicate_records = {'mit': 0, 'cfd': 0}

        for marker in markers:
            for score_name in ('mit', 'cfd'):
                added, duplicates = _merge_score_object(
                    connection,
                    score_name,
                    bucket,
                    marker['outputs'][score_name],
                )
                source_records[score_name] += added + duplicates
                duplicate_records[score_name] += duplicates

        reduced = {}
        for score_name in ('mit', 'cfd'):
            contribution_sum, unique_count = connection.execute(
                f'SELECT COALESCE(SUM(score), 0.0), COUNT(*) FROM {score_name}'
            ).fetchone()
            reduced[score_name] = {
                'score': 10000.0 / (100.0 + contribution_sum),
                'contributionSum': contribution_sum,
                'uniqueOfftargets': unique_count,
                'sourceRecords': source_records[score_name],
                'duplicateRecords': duplicate_records[score_name],
            }
        return reduced
    finally:
        connection.close()


def _result_keys(trigger):
    prefix = (
        f'{trigger["genome"]}/reducer/{trigger["jobId"]}/targets/'
        f'{trigger["targetId"]}'
    )
    return {
        'mit': f'{prefix}/mit.json',
        'cfd': f'{prefix}/cfd.json',
        'result': f'{prefix}/result.json',
    }


def _write_json(bucket, key, document):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(document, separators=(',', ':')).encode('utf-8'),
        ContentType='application/json',
    )


def _reduce(trigger, markers):
    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        reduced = _reduce_markers(
            trigger['bucket'],
            markers,
            os.path.join(directory, 'reducer.sqlite3'),
        )

    keys = _result_keys(trigger)
    common = {
        'schemaVersion': 1,
        'jobId': trigger['jobId'],
        'targetId': trigger['targetId'],
        'genome': trigger['genome'],
        'shardCount': SHARD_COUNT,
    }
    for score_name in ('mit', 'cfd'):
        _write_json(
            trigger['bucket'],
            keys[score_name],
            {**common, 'scoreMethod': score_name, **reduced[score_name]},
        )

    result = {
        **common,
        'status': 'complete',
        'scores': {
            'mit': reduced['mit']['score'],
            'cfd': reduced['cfd']['score'],
        },
        'outputs': {'mit': keys['mit'], 'cfd': keys['cfd']},
    }
    return result, keys


def _completion_id(trigger):
    return f'{trigger["jobId"]}:{trigger["targetId"]}'


def _record_pipeline_completion(trigger, scores):
    if not TARGETS_TABLE or not TASK_TRACKING_TABLE:
        raise ValueError('Reducer DynamoDB table environment is not configured')

    completion_id = _completion_id(trigger)
    for attempt in range(TRANSACTION_MAX_ATTEMPTS):
        try:
            dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    'Update': {
                        'TableName': TARGETS_TABLE,
                        'Key': {
                            'JobID': {'S': trigger['jobId']},
                            'TargetID': {'N': str(trigger['targetId'])},
                        },
                        'UpdateExpression': (
                            'SET IsslScore = :mit, CfdScore = :cfd, '
                            'ReducerTaskId = :task'
                        ),
                        'ConditionExpression': (
                            'attribute_exists(JobID) AND '
                            'attribute_not_exists(ReducerTaskId)'
                        ),
                        'ExpressionAttributeValues': {
                            ':mit': {'S': json.dumps(scores['mit'])},
                            ':cfd': {'S': json.dumps(scores['cfd'])},
                            ':task': {'S': completion_id},
                        },
                    },
                },
                {
                    'Update': {
                        'TableName': TASK_TRACKING_TABLE,
                        'Key': {'JobID': {'S': trigger['jobId']}},
                        'UpdateExpression': (
                            'ADD NumScoredOfftarget :one, #version :one'
                        ),
                        'ConditionExpression': 'attribute_exists(JobID)',
                        'ExpressionAttributeNames': {
                            '#version': 'Version',
                        },
                        'ExpressionAttributeValues': {
                            ':one': {'N': '1'},
                        },
                    },
                },
            ],
            )
            return True
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') != 'TransactionCanceledException':
                raise

            cancellation_reasons = error.response.get('CancellationReasons', [])
            has_conflict = any(
                reason.get('Code') == 'TransactionConflict'
                for reason in cancellation_reasons
            )
            if has_conflict and attempt + 1 < TRANSACTION_MAX_ATTEMPTS:
                delay = random.uniform(
                    0,
                    min(
                        TRANSACTION_BACKOFF_MAX_SECONDS,
                        TRANSACTION_BACKOFF_BASE_SECONDS * (2 ** attempt),
                    ),
                )
                print(json.dumps({
                    'event': 'reducer_transaction_conflict',
                    'jobId': trigger['jobId'],
                    'targetId': trigger['targetId'],
                    'attempt': attempt + 1,
                    'retryDelaySeconds': delay,
                    'cancellationReasons': cancellation_reasons,
                }))
                time.sleep(delay)
                continue

            target = dynamodb_client.get_item(
                TableName=TARGETS_TABLE,
                Key={
                    'JobID': {'S': trigger['jobId']},
                    'TargetID': {'N': str(trigger['targetId'])},
                },
                ConsistentRead=True,
            ).get('Item', {})
            recorded_id = target.get('ReducerTaskId', {}).get('S')
            if recorded_id == completion_id:
                return False
            raise


def lambda_handler(event, context):
    processed = 0
    waiting = 0
    ignored = 0
    for envelope in event.get('Records', []):
        for record in _s3_records(envelope):
            trigger = _mapper_event(record)
            if trigger is None:
                ignored += 1
                continue
            markers = _load_all_markers(trigger)
            if markers is None:
                waiting += 1
                continue
            keys = _result_keys(trigger)
            existing_result = _get_json(
                trigger['bucket'], keys['result'], missing_is_none=True,
            )
            if existing_result is not None:
                processed += 1
                continue

            result, keys = _reduce(trigger, markers)
            pipeline_updated = _record_pipeline_completion(
                trigger, result['scores'],
            )
            result['pipelineStatus'] = 'complete'
            # The final marker is written only after both score objects and the
            # existing DynamoDB completion pipeline have succeeded.
            _write_json(trigger['bucket'], keys['result'], result)
            processed += 1
            print(json.dumps({
                'event': 'reducer_complete',
                'jobId': trigger['jobId'],
                'targetId': trigger['targetId'],
                'scores': result['scores'],
                'pipelineUpdated': pipeline_updated,
            }))
    return {'processed': processed, 'waiting': waiting, 'ignored': ignored}
