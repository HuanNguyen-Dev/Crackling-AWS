"""Dispatch and de-duplicate parallel ISSL index builds."""

import hashlib
import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


BUCKET = os.environ['BUCKET']
BUILD_TABLE_NAME = os.environ['BUILD_TABLE']
WORK_QUEUE = os.environ['WORK_QUEUE']
TARGET_SCAN_QUEUE = os.environ['QUEUE']
COORDINATOR_QUEUE = os.environ['COORDINATOR_QUEUE']
ALGORITHM_VERSION = os.getenv('ISSL_ALGORITHM_VERSION', 'parallel-v1')

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
build_table = boto3.resource('dynamodb').Table(BUILD_TABLE_NAME)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _message_body(record):
    body = json.loads(record['body']) if isinstance(record.get('body'), str) else record['body']
    if 'default' in body:
        body = json.loads(body['default'])
    for field in ('Genome', 'Sequence', 'JobID'):
        if field not in body:
            raise ValueError(f'ISSL creation request is missing {field}')
    return body


def _source_manifest(genome):
    paginator = s3_client.get_paginator('list_objects_v2')
    sources = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f'{genome}/fasta/'):
        for item in page.get('Contents', []):
            if item['Size'] <= 0 or item['Key'].endswith('/'):
                continue
            sources.append({
                'bucket': BUCKET,
                'key': item['Key'],
                'etag': item.get('ETag', '').strip('"'),
                'size': int(item['Size']),
            })
    sources.sort(key=lambda item: item['key'])
    if not sources:
        raise ValueError(f'No FASTA objects found for genome {genome}')
    return sources


def _build_id(genome, sources):
    identity = json.dumps({
        'genome': genome,
        'algorithmVersion': ALGORITHM_VERSION,
        'sources': sources,
    }, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(identity).hexdigest()[:24]


def _send(queue_url, body):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


def _release_ready_request(request, build_id):
    genome = request['Genome']
    job_id = str(request['JobID'])
    key = {'Genome': genome, 'RecordId': f'REQUEST#{build_id}#{job_id}'}
    current = build_table.get_item(Key=key, ConsistentRead=True).get('Item', request)

    if not current.get('coordinatorReleased'):
        _send(COORDINATOR_QUEUE, {
            'schemaVersion': 1,
            'JobID': job_id,
            'Genome': genome,
        })
        build_table.update_item(
            Key=key,
            UpdateExpression='SET coordinatorReleased = :true, updatedAt = :now',
            ExpressionAttributeValues={':true': True, ':now': _now()},
        )

    if not current.get('targetScanReleased'):
        _send(TARGET_SCAN_QUEUE, {
            'Genome': genome,
            'Sequence': request['Sequence'],
            'JobID': job_id,
        })
        build_table.update_item(
            Key=key,
            UpdateExpression='SET targetScanReleased = :true, updatedAt = :now',
            ExpressionAttributeValues={':true': True, ':now': _now()},
        )


def _register_request(request, build_id):
    item = {
        'Genome': request['Genome'],
        'RecordId': f'REQUEST#{build_id}#{request["JobID"]}',
        'buildId': build_id,
        'JobID': str(request['JobID']),
        'Sequence': request['Sequence'],
        'status': 'WAITING',
        'coordinatorReleased': False,
        'targetScanReleased': False,
        'createdAt': _now(),
    }
    try:
        build_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(RecordId)',
        )
        return item
    except ClientError as error:
        if error.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        return build_table.get_item(
            Key={'Genome': item['Genome'], 'RecordId': item['RecordId']},
            ConsistentRead=True,
        )['Item']


def _create_build(genome, build_id, sources):
    record_id = f'BUILD#{build_id}'
    item = {
        'Genome': genome,
        'RecordId': record_id,
        'buildId': build_id,
        'algorithmVersion': ALGORITHM_VERSION,
        'status': 'PLANNING',
        'expectedTasks': len(sources),
        'completedTasks': 0,
        'phase': 'extract',
        'round': 0,
        'sourceManifest': sources,
        'finalKey': f'{genome}/issl/{genome}.issl',
        'createdAt': _now(),
        'updatedAt': _now(),
    }
    try:
        build_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(RecordId)',
        )
        return item, True
    except ClientError as error:
        if error.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        existing = build_table.get_item(
            Key={'Genome': genome, 'RecordId': record_id},
            ConsistentRead=True,
        )['Item']
        return existing, False


def _dispatch_extract_tasks(build, sources):
    genome = build['Genome']
    build_id = build['buildId']
    for index, source in enumerate(sources):
        _send(WORK_QUEUE, {
            'schemaVersion': 1,
            'taskType': 'extract',
            'stage': 'extract',
            'genome': genome,
            'buildId': build_id,
            'taskId': f'extract-{index:06d}',
            'source': source,
            'outputKey': (
                f'{genome}/issl-builds/{build_id}/extract/'
                f'{index:06d}.run'
            ),
        })
    build_table.update_item(
        Key={'Genome': genome, 'RecordId': f'BUILD#{build_id}'},
        UpdateExpression='SET #status = :status, updatedAt = :now',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':status': 'EXTRACTING', ':now': _now()},
    )


def _process(request):
    genome = str(request['Genome'])
    sources = _source_manifest(genome)
    build_id = _build_id(genome, sources)
    waiting_request = _register_request(request, build_id)
    build, created = _create_build(genome, build_id, sources)

    if build.get('status') == 'READY':
        try:
            s3_client.head_object(Bucket=BUCKET, Key=build['finalKey'])
        except ClientError:
            raise RuntimeError('Build is marked READY but its final ISSL is missing')
        _release_ready_request(waiting_request, build_id)
        return

    if created:
        _dispatch_extract_tasks(build, sources)


def lambda_handler(event, context):
    failures = []
    for record in event.get('Records', []):
        try:
            _process(_message_body(record))
        except Exception:
            print(json.dumps({'event': 'issl_dispatch_failed', 'record': record}, default=str))
            failures.append({'itemIdentifier': record.get('messageId', 'unknown')})
    return {'batchItemFailures': failures}
