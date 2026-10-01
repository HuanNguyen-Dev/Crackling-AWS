import json
import os
import shutil
import subprocess
import tempfile
import struct
import boto3

BUCKET = os.environ['BUCKET']
COMPLETION_QUEUE = os.environ['COMPLETION_QUEUE']
BINARY_SOURCE = '/opt/extractor'
BINARY = '/tmp/extractor'
CHUNK = 8 * 1024 * 1024
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _range(source, path):
    start, end = int(source['startByte']), int(source['endByte'])
    if end <= start:
        open(path, 'wb').close()
        return
    response = s3.get_object(Bucket=source.get('bucket', BUCKET), Key=source['key'],
                             Range=f'bytes={start}-{end - 1}')
    with open(path, 'wb') as output:
        while True:
            data = response['Body'].read(CHUNK)
            if not data:
                break
            output.write(data)
    if os.path.getsize(path) != end - start:
        raise ValueError(f'Truncated S3 range for {source["key"]}')


def _download_overflow(catalogue, start_id, end_id, path):
    table = catalogue['overflow']
    base = int(table['startByte'])
    count = int(table['recordCount'])
    if (base < 0 or count < 0 or int(table['recordBytes']) != 16
            or int(table['endByte']) != base + 16 * count):
        raise ValueError('Invalid overflow table contract')
    def lower_bound(target):
        lo, hi = 0, count
        while lo < hi:
            mid = (lo + hi) // 2
            offset = base + 16 * mid
            response = s3.get_object(
                Bucket=catalogue['bucket'], Key=catalogue['key'],
                Range=f'bytes={offset}-{offset + 7}')
            body = response['Body']
            try:
                data = body.read()
            finally:
                body.close()
            if len(data) != 8:
                raise ValueError('Truncated overflow ID')
            global_id, = struct.unpack('<Q', data)
            if global_id < target:
                lo = mid + 1
            else:
                hi = mid
        return lo

    first = lower_bound(start_id)
    last = lower_bound(end_id)
    if last < first or last - first > end_id - start_id:
        raise ValueError('Invalid overflow partition size')
    _range({
        'bucket': catalogue['bucket'], 'key': catalogue['key'],
        'startByte': base + 16 * first,
        'endByte': base + 16 * last,
    }, path)


def _process(task):
    if task.get('schemaVersion') != 3:
        raise ValueError('Unsupported Extractor task schemaVersion')
    id_bits = int(task['idBits'])
    record_bytes = int(task['hydratedRecordBytes'])
    if id_bits not in (32, 64) or record_bytes != 12 + id_bits // 8:
        raise ValueError('Extractor hydrated-record width does not match ID width')
    if not os.path.exists(BINARY):
        shutil.copyfile(BINARY_SOURCE, BINARY)
        os.chmod(BINARY, 0o755)
    records = []
    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        signatures = os.path.join(directory, 'signatures.bin')
        occurrences = os.path.join(directory, 'occurrences.bin')
        overflow = os.path.join(directory, 'overflow.bin')
        bucket_file = os.path.join(directory, 'bucket.bin')
        output_file = os.path.join(directory, 'candidates.bin')
        common = {'bucket': task['catalogue']['bucket'],
                  'key': task['catalogue']['key']}
        _range({**common, **task['catalogue']['signatures']}, signatures)
        _range({**common, **task['catalogue']['occurrences']}, occurrences)
        _download_overflow(task['catalogue'], int(task['idRange']['start']),
                           int(task['idRange']['end']), overflow)
        catalogue_bytes = sum(os.path.getsize(path)
                              for path in (signatures, occurrences, overflow))
        total_bucket_bytes = 0
        total_output_bytes = 0
        peak_local_bytes = catalogue_bytes
        previous_output_bytes = 0
        for bucket in task['buckets']:
            _range({'bucket': task['catalogue'].get('bucket', BUCKET),
                    'key': task['catalogue']['key'],
                    'startByte': bucket['startByte'], 'endByte': bucket['endByte']}, bucket_file)
            bucket_bytes = os.path.getsize(bucket_file)
            # The preceding output remains on /tmp until the native process
            # truncates it, so include that short overlap in the observed peak.
            peak_local_bytes = max(
                peak_local_bytes,
                catalogue_bytes + bucket_bytes + previous_output_bytes,
            )
            completed = subprocess.run([
                BINARY, signatures, occurrences, overflow, bucket_file,
                str(bucket['elementCount']), str(task['idRange']['start']),
                str(task['idRange']['end']), str(task['idBits']),
                str(task['offtargetsCount']), output_file,
            ], capture_output=True, text=True, check=False)
            if completed.returncode:
                raise RuntimeError(f'Extractor failed: {completed.stderr}')
            size = os.path.getsize(output_file)
            if size % record_bytes:
                raise ValueError('Extractor output is not aligned to record format')
            key = (f'{bucket["cachePrefix"]}/parts/'
                   f'{task["idRange"]["start"]}-{task["idRange"]["end"]}.bin')
            count = size // record_bytes
            total_bucket_bytes += bucket_bytes
            total_output_bytes += size
            local_bytes = catalogue_bytes + bucket_bytes + size
            peak_local_bytes = max(peak_local_bytes, local_bytes)
            previous_output_bytes = size
            print(json.dumps({
                'event': 'extractor_bucket_sizes',
                'batchId': task['batchId'],
                'partId': task['partId'],
                'sliceId': bucket['sliceId'],
                'bucketId': bucket['bucketId'],
                'catalogueBytes': catalogue_bytes,
                'bucketBytes': bucket_bytes,
                'hydratedOutputBytes': size,
                'localBytes': local_bytes,
                'recordBytes': record_bytes,
                'idBits': id_bits,
            }))
            if count:
                s3.upload_file(output_file, BUCKET, key,
                               ExtraArgs={'ContentType': 'application/octet-stream'})
            records.append({'sliceId': bucket['sliceId'], 'bucketId': bucket['bucketId'],
                            'manifestKey': bucket['manifestKey'], 'key': key if count else None,
                            'startId': task['idRange']['start'], 'endId': task['idRange']['end'],
                            'recordCount': count, 'recordBytes': record_bytes, 'idBits': id_bits})
        print(json.dumps({
            'event': 'extractor_part_sizes',
            'batchId': task['batchId'],
            'partId': task['partId'],
            'expectedParts': task['expectedParts'],
            'catalogueBytes': catalogue_bytes,
            'bucketBytesProcessed': total_bucket_bytes,
            'hydratedOutputBytes': total_output_bytes,
            'peakLocalBytes': peak_local_bytes,
        }))
    marker_key = f'{task["completionPrefix"]}/{task["partId"]}/result.json'
    marker = {'schemaVersion': 2, 'batchId': task['batchId'], 'partId': task['partId'],
              'expectedParts': task['expectedParts'], 'buckets': records}
    s3.put_object(Bucket=BUCKET, Key=marker_key,
                  Body=json.dumps(marker, separators=(',', ':')).encode(),
                  ContentType='application/json')
    sqs.send_message(QueueUrl=COMPLETION_QUEUE,
                     MessageBody=json.dumps({'batchKey': task['batchKey'],
                                             'completionPrefix': task['completionPrefix']}))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        message = json.loads(record['body'])

        if message.get('taskType') == 'extractorReference':
            if message.get('schemaVersion') != 1:
                raise ValueError('Unsupported extractor reference version')

            batch = json.loads(s3.get_object(
                Bucket=message['bucket'],
                Key=message['batchKey'],
            )['Body'].read())

            index = message['extractorTaskIndex']
            tasks = batch['extractorTasks']
            if type(index) is not int or not 0 <= index < len(tasks):
                raise ValueError('Invalid extractor task index')

            task = tasks[index]
        else:
            # Cached batch.json file
            task = message

        _process(task)

    return {'processed': len(event.get('Records', []))}
