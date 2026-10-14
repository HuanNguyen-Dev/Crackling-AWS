"""Persist schema-7 Mapper batch scores; scoring and deduplication stay in Mapper."""

import hashlib
import json
import math
import os
import random
import re
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


TRANSACTION_MAX_ATTEMPTS = 5
TRANSACTION_BACKOFF_BASE_SECONDS = 0.05
TRANSACTION_BACKOFF_MAX_SECONDS = 1.0
TARGETS_TABLE = os.getenv('TARGETS_TABLE')
TASK_TRACKING_TABLE = os.getenv('TASK_TRACKING_TABLE')
MAPPER_MARKER_PATTERN = re.compile(
    r'^(?P<genome>.+)/mapper/(?P<job_id>[^/]+)/batches/'
    r'(?P<batch_id>[^/]+)/mapper-result\.json$'
)

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')


def _mapper_event(record):
    key = unquote_plus(record['s3']['object']['key'])
    match = MAPPER_MARKER_PATTERN.fullmatch(key)
    if not match:
        # Old per-guide/shard notifications must not enter batch finalization.
        return None
    values = match.groupdict()
    return {
        'bucket': record['s3']['bucket']['name'],
        'key': key,
        'genome': values['genome'],
        'jobId': values['job_id'],
        'batchId': values['batch_id'],
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


def _finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _validate_batch(trigger, marker):
    """Validate every row before any persistence, including disabled methods."""
    if not isinstance(marker, dict):
        raise ValueError('Mapper batch marker must be an object')
    expected = {
        'schemaVersion': 7,
        'status': 'complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'genome': trigger['genome'],
    }
    for field, value in expected.items():
        if marker.get(field) != value:
            raise ValueError(f'Mapper batch marker has invalid {field}')
    if type(marker['schemaVersion']) is not int:
        raise ValueError('Mapper schemaVersion must be integer 7')

    scoring = marker.get('scoring')
    if not isinstance(scoring, dict):
        raise ValueError('Mapper batch marker requires scoring configuration')
    distance = scoring.get('maxDistance')
    threshold = scoring.get('scoreThreshold')
    method = scoring.get('scoreMethod')
    if type(distance) is not int or not 0 <= distance <= 4:
        raise ValueError('Mapper maxDistance must be an integer from 0 to 4')
    if not _finite_number(threshold) or not 0 <= threshold <= 100:
        raise ValueError('Mapper scoreThreshold must be finite from 0 to 100')
    if method not in ('mit', 'cfd', 'and', 'or', 'avg'):
        raise ValueError('Mapper scoreMethod is invalid')
    semantics = 'complete' if threshold == 0 else 'thresholded'
    if marker.get('scoreSemantics') != semantics:
        raise ValueError('Mapper scoreSemantics does not match its threshold')

    rows = marker.get('scores')
    if not isinstance(rows, list) or not rows:
        raise ValueError('Mapper batch requires a nonempty score list')
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('Mapper score row must be an object')
        target_id = row.get('targetId')
        if type(target_id) is not int or target_id < 0 or target_id in seen:
            raise ValueError('Mapper Target IDs must be unique nonnegative integers')
        seen.add(target_id)
        for name in ('mit', 'cfd'):
            score = row.get(name)
            disabled = (method == 'mit' and name == 'cfd') or (
                method == 'cfd' and name == 'mit'
            )
            if not _finite_number(score) or (
                score != -1 if disabled else not 0 <= score <= 100
            ):
                raise ValueError(f'Mapper target {target_id} has invalid {name} score')
    return marker


def _result_key(trigger):
    return (
        f'{trigger["genome"]}/reducer/{trigger["jobId"]}/batches/'
        f'{trigger["batchId"]}/result.json'
    )


def _write_json(bucket, key, document):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(document, separators=(',', ':'), allow_nan=False).encode('utf-8'),
        ContentType='application/json',
    )


def _completion_id(trigger):
    return f'{trigger["jobId"]}:{trigger["targetId"]}'


def _matching_target(target, completion_id, scores):
    if target.get('ReducerTaskId', {}).get('S') != completion_id:
        return False
    for attribute, name in (('IsslScore', 'mit'), ('CfdScore', 'cfd')):
        try:
            stored = json.loads(target[attribute]['S'])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError('Completed target has invalid persisted scores') from error
        if not _finite_number(stored) or stored != scores[name]:
            raise ValueError('Completed target scores conflict with Mapper batch')
    return True


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
                            'ExpressionAttributeNames': {'#version': 'Version'},
                            'ExpressionAttributeValues': {':one': {'N': '1'}},
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
            if _matching_target(target, completion_id, scores):
                return False
            raise


def _process_s3_record(record):
    trigger = _mapper_event(record)
    if trigger is None:
        return 'ignored'
    marker = _validate_batch(trigger, _get_json(trigger['bucket'], trigger['key']))
    # Detect replacement/corruption of a deterministic Mapper result key.
    source_digest = hashlib.sha256(json.dumps(
        marker, sort_keys=True, separators=(',', ':'), allow_nan=False,
    ).encode('utf-8')).hexdigest()
    result = {
        'schemaVersion': 7,
        'status': 'complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'genome': trigger['genome'],
        'sourceMapperKey': trigger['key'],
        'sourceDigest': source_digest,
        'guideCount': len(marker['scores']),
        'scoring': marker['scoring'],
        'scoreSemantics': marker['scoreSemantics'],
    }
    key = _result_key(trigger)
    existing = _get_json(trigger['bucket'], key, missing_is_none=True)
    if existing is not None:
        if existing != result:
            raise ValueError('Batch finalization marker conflicts with Mapper result')
        return 'processed'

    updated = 0
    for row in marker['scores']:
        updated += _record_pipeline_completion(
            {**trigger, 'targetId': row['targetId']},
            {'mit': row['mit'], 'cfd': row['cfd']},
        )
    # A partial DynamoDB failure leaves no marker; retry resumes via target guards.
    _write_json(trigger['bucket'], key, result)
    print(json.dumps({
        'event': 'reducer_batch_complete',
        'jobId': trigger['jobId'],
        'batchId': trigger['batchId'],
        'guideCount': len(marker['scores']),
        'targetsUpdated': updated,
    }))
    return 'processed'


def lambda_handler(event, context):
    counts = {'processed': 0, 'ignored': 0}
    batch_item_failures = []
    for envelope in event.get('Records', []):
        try:
            for record in _s3_records(envelope):
                counts[_process_s3_record(record)] += 1
        except Exception as error:
            message_id = envelope.get('messageId')
            if message_id is None:
                raise
            print(json.dumps({
                'event': 'reducer_message_failed',
                'messageId': message_id,
                'errorType': type(error).__name__,
                'error': str(error),
            }))
            batch_item_failures.append({'itemIdentifier': message_id})
    counts['batchItemFailures'] = batch_item_failures
    return counts
