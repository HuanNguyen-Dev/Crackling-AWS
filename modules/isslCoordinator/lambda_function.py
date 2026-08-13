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
MAPPER_QUEUE_URL = os.environ['MAPPER_QUEUE']
NUM_SHARDS = int(os.getenv('NUM_SHARDS', '5'))
MAX_DISTANCE = int(os.getenv('MAX_DISTANCE', '4'))
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '75'))
SCORE_METHOD = os.getenv('SCORE_METHOD', 'mit')

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

    if slice_count < NUM_SHARDS:
        raise ValueError(
            f'ISSL contains {slice_count} slices; Stage 1 requires exactly '
            f'{NUM_SHARDS} non-empty shards'
        )

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
    required_boundaries = {
        ((shard * slice_count) // NUM_SHARDS) * slice_limit
        for shard in range(NUM_SHARDS + 1)
    }

    cumulative_bytes = {}
    running_bytes = 0
    for index, bucket_size in enumerate(layout['slicelistSizes']):
        if index in required_boundaries:
            cumulative_bytes[index] = running_bytes
        running_bytes += bucket_size * UINT64_BYTES
    cumulative_bytes[len(layout['slicelistSizes'])] = running_bytes

    shards = []
    for shard_id in range(NUM_SHARDS):
        start_slice = (shard_id * slice_count) // NUM_SHARDS
        end_slice = ((shard_id + 1) * slice_count) // NUM_SHARDS
        shards.append({
            'shardId': shard_id,
            'startSlice': start_slice,
            'endSlice': end_slice,
            'startByte': (
                layout['baseOffsetBytes']
                + cumulative_bytes[start_slice * slice_limit]
            ),
            'endByte': (
                layout['baseOffsetBytes']
                + cumulative_bytes[end_slice * slice_limit]
            ),
        })
    return shards


def _parse_record(record):
    message = json.loads(record['body'])
    if message.get('schemaVersion') != 1:
        raise ValueError('Unsupported or missing Coordinator message schemaVersion')

    for field in ('JobID', 'Genome'):
        if field not in message:
            raise ValueError(f'Coordinator message is missing {field}')
    return message


def _mapper_task(message, issl_key, output_prefix, shard):
    return {
        'schemaVersion': 1,
        'jobId': message['JobID'],
        'genome': message['Genome'],
        'shardId': shard['shardId'],
        'shardCount': NUM_SHARDS,
        'startSlice': shard['startSlice'],
        'endSlice': shard['endSlice'],
        'startByte': shard['startByte'],
        'endByte': shard['endByte'],
        'issl': {
            'bucket': BUCKET,
            'key': issl_key,
        },
        'query': {
            'type': 'dynamodb',
            'jobId': message['JobID'],
            'status': 'pending-target-scan',
        },
        'scoring': {
            'maxDistance': MAX_DISTANCE,
            'scoreThreshold': SCORE_THRESHOLD,
            'scoreMethod': SCORE_METHOD,
        },
        'output': {
            'bucket': BUCKET,
            'prefix': f'{output_prefix}/mapper',
        },
    }


def _process_job(message):
    job_id = str(message['JobID'])
    genome = str(message['Genome'])
    issl_key = f'{genome}/issl/{genome}.issl'
    output_prefix = f'{genome}/coordinator/{job_id}'

    layout = _read_issl_layout(issl_key)
    shards = _calculate_shards(layout)
    tasks = [
        _mapper_task(message, issl_key, output_prefix, shard)
        for shard in shards
    ]

    audit_document = {
        'schemaVersion': 1,
        'jobId': job_id,
        'genome': genome,
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

    response = sqs_client.send_message_batch(
        QueueUrl=MAPPER_QUEUE_URL,
        Entries=[
            {
                'Id': str(task['shardId']),
                'MessageBody': json.dumps(task),
            }
            for task in tasks
        ],
    )
    if response.get('Failed'):
        raise RuntimeError(
            f'Failed to publish Mapper tasks: {json.dumps(response["Failed"])}'
        )
    successful_count = len(response.get('Successful', []))
    if successful_count != NUM_SHARDS:
        raise RuntimeError(
            f'Expected {NUM_SHARDS} successful Mapper messages, received '
            f'{successful_count}'
        )

    print(json.dumps({
        'event': 'mapper_tasks_published',
        'jobId': job_id,
        'count': successful_count,
        'queueUrl': MAPPER_QUEUE_URL,
    }))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process_job(_parse_record(record))

    return {'processed': len(event.get('Records', []))}
