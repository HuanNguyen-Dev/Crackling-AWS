import json
import os
import struct

import boto3


ISSL_HEADER_FIELDS = 6
SIZE_T_BYTES = 8
UINT64_BYTES = 8
DOUBLE_BYTES = 8
ISSL_HEADER_BYTES = ISSL_HEADER_FIELDS * SIZE_T_BYTES

BUCKET = os.environ['BUCKET']
TARGET_SCAN_QUEUE_URL = os.environ['TARGET_SCAN_QUEUE']
NUM_SHARDS = int(os.getenv('NUM_SHARDS', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def _read_s3_range(key, start, length):
    if length <= 0:
        return b''

    response = s3_client.get_object(
        Bucket=BUCKET,
        Key=key,
        Range=f'bytes={start}-{start + length - 1}',
    )
    data = response['Body'].read()
    if len(data) != length:
        raise ValueError(
            f'Expected {length} bytes from s3://{BUCKET}/{key} at offset '
            f'{start}, received {len(data)}'
        )
    return data


def _read_issl_layout(key):
    # Crackling ISSL files are produced and consumed on x86_64 Linux. The six
    # native size_t header fields are therefore little-endian unsigned 64-bit
    # integers, matching the current C++ Coordinator implementation.
    header_data = _read_s3_range(key, 0, ISSL_HEADER_BYTES)
    (
        offtargets_count,
        sequence_length,
        sequence_count,
        slice_width,
        slice_count,
        scores_count,
    ) = struct.unpack('<6Q', header_data)

    if slice_count != NUM_SHARDS:
        raise ValueError(
            f'ISSL contains {slice_count} slices; bucket-level scoring '
            f'requires exactly {NUM_SHARDS}'
        )
    if MAX_DISTANCE != 4:
        raise ValueError('Bucket-level scoring requires MAX_DISTANCE=4')

    slice_limit = 1 << slice_width
    slicelist_size_count = slice_count * slice_limit
    slicelist_sizes_offset = (
        ISSL_HEADER_BYTES
        + scores_count * (UINT64_BYTES + DOUBLE_BYTES)
        + offtargets_count * UINT64_BYTES
    )
    slicelist_sizes_bytes = slicelist_size_count * SIZE_T_BYTES
    slicelist_sizes_data = _read_s3_range(
        key,
        slicelist_sizes_offset,
        slicelist_sizes_bytes,
    )
    slicelist_sizes = struct.unpack(
        f'<{slicelist_size_count}Q',
        slicelist_sizes_data,
    )

    base_offset_bytes = slicelist_sizes_offset + slicelist_sizes_bytes
    return {
        'offtargetsCount': offtargets_count,
        'sequenceLength': sequence_length,
        'sequenceCount': sequence_count,
        'sliceWidth': slice_width,
        'sliceCount': slice_count,
        'scoresCount': scores_count,
        'sliceLimit': slice_limit,
        'baseOffsetBytes': base_offset_bytes,
        'slicelistSizes': slicelist_sizes,
    }


def _calculate_shards(layout):
    slice_count = layout['sliceCount']
    slice_limit = layout['sliceLimit']
    running_byte = layout['baseOffsetBytes']
    shards = []
    for shard_id in range(NUM_SHARDS):
        bucket_offsets = [running_byte]
        first_bucket = shard_id * slice_limit
        for bucket_size in layout['slicelistSizes'][
            first_bucket:first_bucket + slice_limit
        ]:
            running_byte += bucket_size * UINT64_BYTES
            bucket_offsets.append(running_byte)
        shards.append({
            'shardId': shard_id,
            'sliceId': shard_id,
            'bucketOffsets': bucket_offsets,
        })
    return shards


def _parse_record(record):
    message = json.loads(record['body'])
    if message.get('schemaVersion') != 1:
        raise ValueError('Unsupported or missing Coordinator message schemaVersion')

    for field in ('JobID', 'Genome', 'Sequence'):
        if field not in message:
            raise ValueError(f'Coordinator message is missing {field}')
    return message


def _process_job(message):
    job_id = str(message['JobID'])
    genome = str(message['Genome'])
    issl_key = f'{genome}/issl/{genome}.issl'
    output_prefix = f'{genome}/coordinator/{job_id}'

    layout = _read_issl_layout(issl_key)
    shards = _calculate_shards(layout)
    audit_document = {
        'schemaVersion': 2,
        'jobId': job_id,
        'genome': genome,
        'maxDistance': MAX_DISTANCE,
        'issl': {'bucket': BUCKET, 'key': issl_key},
        'layout': {
            key: value
            for key, value in layout.items()
            if key != 'slicelistSizes'
        },
        'shards': shards,
    }
    audit_key = f'{genome}/issl/shards.json'
    s3_client.put_object(
        Bucket=BUCKET,
        Key=audit_key,
        Body=json.dumps(audit_document, indent=2).encode('utf-8'),
        ContentType='application/json',
    )

    print(json.dumps({
        'event': 'coordinator_shards_calculated',
        'jobId': job_id,
        'genome': genome,
        'isslKey': issl_key,
        'auditKey': audit_key,
        'shards': shards,
    }))

    # Target Scan is released only after the complete shard manifest exists.
    # Its algorithm and input contract remain unchanged.
    response = sqs_client.send_message(
        QueueUrl=TARGET_SCAN_QUEUE_URL,
        MessageBody=json.dumps({
            'Genome': genome,
            'Sequence': message['Sequence'],
            'JobID': job_id,
        }),
    )

    print(json.dumps({
        'event': 'target_scan_released',
        'jobId': job_id,
        'messageId': response.get('MessageId'),
        'queueUrl': TARGET_SCAN_QUEUE_URL,
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process_job(_parse_record(record))

    return {'processed': len(event.get('Records', []))}
