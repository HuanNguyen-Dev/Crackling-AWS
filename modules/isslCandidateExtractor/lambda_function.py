import json
import os
import shutil
import subprocess
import tempfile

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


def _process(task):
    if task.get('schemaVersion') != 1:
        raise ValueError('Unsupported Extractor task schemaVersion')
    if not os.path.exists(BINARY):
        shutil.copyfile(BINARY_SOURCE, BINARY)
        os.chmod(BINARY, 0o755)
    records = []
    with tempfile.TemporaryDirectory(dir='/tmp') as directory:
        catalogue = os.path.join(directory, 'catalogue.bin')
        bucket_file = os.path.join(directory, 'bucket.bin')
        output_file = os.path.join(directory, 'candidates.bin')
        _range(task['catalogue'], catalogue)
        catalogue_bytes = os.path.getsize(catalogue)
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
                BINARY, catalogue, bucket_file,
                str(task['idRange']['start']), str(task['idRange']['end']), output_file,
            ], capture_output=True, text=True, check=False)
            if completed.returncode:
                raise RuntimeError(f'Extractor failed: {completed.stderr}')
            size = os.path.getsize(output_file)
            if size % 16:
                raise ValueError('Extractor output is not aligned to <QII> records')
            key = (f'{bucket["cachePrefix"]}/parts/'
                   f'{task["idRange"]["start"]}-{task["idRange"]["end"]}.bin')
            count = size // 16
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
            }))
            if count:
                s3.upload_file(output_file, BUCKET, key,
                               ExtraArgs={'ContentType': 'application/octet-stream'})
            records.append({'sliceId': bucket['sliceId'], 'bucketId': bucket['bucketId'],
                            'manifestKey': bucket['manifestKey'], 'key': key if count else None,
                            'startId': task['idRange']['start'], 'endId': task['idRange']['end'],
                            'recordCount': count, 'recordBytes': 16})
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
    marker = {'schemaVersion': 1, 'batchId': task['batchId'], 'partId': task['partId'],
              'expectedParts': task['expectedParts'], 'buckets': records}
    s3.put_object(Bucket=BUCKET, Key=marker_key,
                  Body=json.dumps(marker, separators=(',', ':')).encode(),
                  ContentType='application/json')
    sqs.send_message(QueueUrl=COMPLETION_QUEUE,
                     MessageBody=json.dumps({'batchKey': task['batchKey'],
                                             'completionPrefix': task['completionPrefix']}))


def lambda_handler(event, context):
    for record in event.get('Records', []):
        _process(json.loads(record['body']))
    return {'processed': len(event.get('Records', []))}
