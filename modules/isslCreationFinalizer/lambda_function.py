"""Assemble scorer-compatible ISSL components and release waiting jobs."""

import json
import math
import os
import struct
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key


BUCKET = os.environ['BUCKET']
BUILD_TABLE_NAME = os.environ['BUILD_TABLE']
TARGET_SCAN_QUEUE = os.environ['TARGET_SCAN_QUEUE']
COORDINATOR_QUEUE = os.environ['COORDINATOR_QUEUE']
SEQUENCE_LENGTH = 20
SLICE_WIDTH = 8
SLICE_COUNT = 5
PART_SIZE = 8 * 1024 * 1024

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
build_table = boto3.resource('dynamodb').Table(BUILD_TABLE_NAME)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _send(queue_url, body):
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


class MultipartWriter:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        response = s3_client.create_multipart_upload(
            Bucket=bucket, Key=key, ContentType='application/octet-stream'
        )
        self.upload_id = response['UploadId']
        self.buffer = bytearray()
        self.parts = []

    def write(self, data):
        self.buffer.extend(data)
        while len(self.buffer) >= PART_SIZE:
            self._upload(bytes(self.buffer[:PART_SIZE]))
            del self.buffer[:PART_SIZE]

    def _upload(self, data):
        part_number = len(self.parts) + 1
        response = s3_client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        self.parts.append({'PartNumber': part_number, 'ETag': response['ETag']})

    def complete(self):
        if self.buffer or not self.parts:
            self._upload(bytes(self.buffer))
            self.buffer.clear()
        s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts},
        )

    def abort(self):
        s3_client.abort_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
        )


def _masks_two_bit(length, mismatches):
    if mismatches < length:
        if mismatches > 0:
            return [
                (1 << ((length - 1) * 2)) + mask
                for mask in _masks_two_bit(length - 1, mismatches - 1)
            ] + _masks_two_bit(length - 1, mismatches)
        return [0]
    mask = 0
    for index in range(length):
        mask |= 1 << (index * 2)
    return [mask]


def _single_score(mismatches):
    penalties = [
        0.0, 0.0, 0.014, 0.0, 0.0, 0.395, 0.317, 0.0, 0.389, 0.079,
        0.445, 0.508, 0.613, 0.851, 0.732, 0.828, 0.615, 0.804, 0.685, 0.583,
    ]
    term1 = 1.0
    for mismatch in mismatches:
        term1 *= 1.0 - penalties[mismatch]
    distance = 19.0 if len(mismatches) == 1 else (
        sum(b - a for a, b in zip(mismatches, mismatches[1:]))
        / (len(mismatches) - 1)
    )
    term2 = 1.0 / (((19.0 - distance) / 19.0) * 4.0 + 1.0)
    return term1 * term2 * (1.0 / (len(mismatches) ** 2)) * 100.0


def _score_pairs():
    scores = {}
    for distance in range(1, SLICE_COUNT):
        for mask in _masks_two_bit(SEQUENCE_LENGTH, distance):
            mismatches = [
                index for index in range(SEQUENCE_LENGTH)
                if (mask >> (index * 2)) & 0x3
            ]
            scores[mask] = _single_score(mismatches)
    return sorted(scores.items())


def _read_exact(stream, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise ValueError(f'Unexpected end of component; wanted {remaining} bytes')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def _catalog_info(key):
    stream = s3_client.get_object(Bucket=BUCKET, Key=key)['Body']
    if _read_exact(stream, 8) != b'ISSLCAT1':
        raise ValueError('Invalid catalogue magic')
    sequence_length, sequence_count, distinct_count = struct.unpack(
        '<3Q', _read_exact(stream, 24)
    )
    if sequence_length != SEQUENCE_LENGTH:
        raise ValueError('Unexpected catalogue sequence length')
    return stream, sequence_count, distinct_count


def _slice_info(key, expected_slice):
    stream = s3_client.get_object(Bucket=BUCKET, Key=key)['Body']
    if _read_exact(stream, 8) != b'ISSLSLC1':
        raise ValueError('Invalid slice magic')
    sequence_length, slice_width, slice_id, bucket_count, distinct_count = struct.unpack(
        '<5Q', _read_exact(stream, 40)
    )
    if (sequence_length, slice_width, slice_id, bucket_count) != (
        SEQUENCE_LENGTH, SLICE_WIDTH, expected_slice, 1 << SLICE_WIDTH
    ):
        raise ValueError(f'Invalid metadata for slice {expected_slice}')
    counts = _read_exact(stream, bucket_count * 8)
    return stream, distinct_count, counts


def _copy_stream(writer, stream, expected_bytes):
    copied = 0
    while copied < expected_bytes:
        chunk = stream.read(min(PART_SIZE, expected_bytes - copied))
        if not chunk:
            raise ValueError('Component payload ended early')
        writer.write(chunk)
        copied += len(chunk)
    if stream.read(1):
        raise ValueError('Component payload is longer than its declared layout')


def _assemble(message):
    catalog_stream, sequence_count, distinct_count = _catalog_info(message['catalogKey'])
    if int(message['sequenceCount']) != sequence_count or int(message['distinctCount']) != distinct_count:
        raise ValueError('Finalization message does not match catalogue counts')

    slices = []
    all_counts = []
    for slice_id, key in enumerate(message['sliceKeys']):
        stream, slice_distinct_count, counts = _slice_info(key, slice_id)
        if slice_distinct_count != distinct_count:
            raise ValueError(f'Slice {slice_id} distinct count does not match catalogue')
        slices.append(stream)
        all_counts.append(counts)

    score_pairs = _score_pairs()
    writer = MultipartWriter(BUCKET, message['finalKey'])
    try:
        writer.write(struct.pack(
            '<6Q', distinct_count, SEQUENCE_LENGTH, sequence_count,
            SLICE_WIDTH, SLICE_COUNT, len(score_pairs),
        ))
        for mask, score in score_pairs:
            writer.write(struct.pack('<Qd', mask, score))

        # Catalogue entries contain <signature:uint64, occurrences:uint32>.
        # Only signatures are stored in this section of the legacy ISSL.
        for _ in range(distinct_count):
            signature, _occurrences = struct.unpack('<QI', _read_exact(catalog_stream, 12))
            writer.write(struct.pack('<Q', signature))
        if catalog_stream.read(1):
            raise ValueError('Catalogue contains trailing data')

        for counts in all_counts:
            writer.write(counts)
        for stream in slices:
            _copy_stream(writer, stream, distinct_count * 8)
        writer.complete()
    except Exception:
        writer.abort()
        raise
    return sequence_count, distinct_count


def _release_requests(genome, build_id):
    expression = (
        Key('Genome').eq(genome)
        & Key('RecordId').begins_with(f'REQUEST#{build_id}#')
    )
    requests = []
    start_key = None
    while True:
        arguments = {'KeyConditionExpression': expression, 'ConsistentRead': True}
        if start_key:
            arguments['ExclusiveStartKey'] = start_key
        response = build_table.query(**arguments)
        requests.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey')
        if not start_key:
            break
    for request in requests:
        key = {'Genome': genome, 'RecordId': request['RecordId']}
        if not request.get('coordinatorReleased'):
            _send(COORDINATOR_QUEUE, {
                'schemaVersion': 1,
                'JobID': request['JobID'],
                'Genome': genome,
            })
            build_table.update_item(
                Key=key,
                UpdateExpression='SET coordinatorReleased = :true, updatedAt = :now',
                ExpressionAttributeValues={':true': True, ':now': _now()},
            )
        if not request.get('targetScanReleased'):
            _send(TARGET_SCAN_QUEUE, {
                'Genome': genome,
                'Sequence': request['Sequence'],
                'JobID': request['JobID'],
            })
            build_table.update_item(
                Key=key,
                UpdateExpression=(
                    'SET targetScanReleased = :true, #status = :released, updatedAt = :now'
                ),
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={
                    ':true': True, ':released': 'RELEASED', ':now': _now()
                },
            )


def _process(message):
    if message.get('schemaVersion') != 1 or len(message.get('sliceKeys', [])) != SLICE_COUNT:
        raise ValueError('Invalid finalization message')
    sequence_count, distinct_count = _assemble(message)
    build_table.update_item(
        Key={
            'Genome': message['genome'],
            'RecordId': f'BUILD#{message["buildId"]}',
        },
        UpdateExpression=(
            'SET #status = :ready, phase = :ready, sequenceCount = :sequences, '
            'distinctCount = :distinct, readyAt = :now, updatedAt = :now'
        ),
        ConditionExpression='phase = :finalize OR #status = :ready',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':ready': 'READY',
            ':finalize': 'finalize',
            ':sequences': sequence_count,
            ':distinct': distinct_count,
            ':now': _now(),
        },
    )
    _release_requests(message['genome'], message['buildId'])


def lambda_handler(event, context):
    failures = []
    for record in event.get('Records', []):
        try:
            _process(json.loads(record['body']))
        except Exception:
            failures.append({'itemIdentifier': record.get('messageId', 'unknown')})
            print(json.dumps({'event': 'issl_finalize_failed', 'record': record}, default=str))
    return {'batchItemFailures': failures}
