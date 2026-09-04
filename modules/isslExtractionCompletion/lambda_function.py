import json
import os

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ['BUCKET']
MAPPER_QUEUE = os.environ['MAPPER_QUEUE']
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)['Body'].read())
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey'):
            return None
        raise


def _send(tasks):
    response = sqs.send_message_batch(QueueUrl=MAPPER_QUEUE, Entries=[
        {'Id': str(index), 'MessageBody': json.dumps(task, separators=(',', ':'))}
        for index, task in enumerate(tasks)
    ])
    if response.get('Failed') or len(response.get('Successful', [])) != len(tasks):
        raise RuntimeError('Failed to publish every Mapper task')


def _process(message):
    batch = _json(message['batchKey'])
    if not batch:
        raise ValueError('Extraction batch document is missing')
    dispatched_key = message['batchKey'].removesuffix('batch.json') + 'dispatched.json'
    if _json(dispatched_key) is not None:
        return True
    parts = []
    for part_id in range(int(batch['expectedParts'])):
        marker = _json(f'{message["completionPrefix"]}/{part_id}/result.json')
        if marker is None:
            return False
        parts.append(marker)

    for bucket in batch['missingBuckets']:
        fragments = []
        for part in parts:
            fragment = next(item for item in part['buckets']
                            if int(item['sliceId']) == int(bucket['sliceId'])
                            and int(item['bucketId']) == int(bucket['bucketId']))
            fragments.append(fragment)
        manifest = {
            'schemaVersion': 1, 'sliceId': bucket['sliceId'], 'bucketId': bucket['bucketId'],
            'recordFormat': {'endianness': 'little', 'recordBytes': 16,
                             'fields': ['signature:uint64', 'globalId:uint32', 'occurrences:uint32']},
            'recordCount': sum(int(item['recordCount']) for item in fragments),
            'parts': fragments,
        }
        s3.put_object(Bucket=BUCKET, Key=bucket['manifestKey'],
                      Body=json.dumps(manifest, separators=(',', ':')).encode(),
                      ContentType='application/json')
    _send(batch['mapperTasks'])
    s3.put_object(Bucket=BUCKET, Key=dispatched_key,
                  Body=json.dumps({'schemaVersion': 1,
                                   'batchId': batch['batchId']},
                                  separators=(',', ':')).encode(),
                  ContentType='application/json')
    return True


def lambda_handler(event, context):
    ready = 0
    messages = {}
    for record in event.get('Records', []):
        message = json.loads(record['body'])
        messages[message['batchKey']] = message
    for message in messages.values():
        ready += int(_process(message))
    return {'processed': len(event.get('Records', [])), 'ready': ready}
