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
        for bucket in task['buckets']:
            _range({'bucket': task['catalogue'].get('bucket', BUCKET),
                    'key': task['catalogue']['key'],
                    'startByte': bucket['startByte'], 'endByte': bucket['endByte']}, bucket_file)
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
            if count:
                s3.upload_file(output_file, BUCKET, key,
                               ExtraArgs={'ContentType': 'application/octet-stream'})
            records.append({'sliceId': bucket['sliceId'], 'bucketId': bucket['bucketId'],
                            'manifestKey': bucket['manifestKey'], 'key': key if count else None,
                            'startId': task['idRange']['start'], 'endId': task['idRange']['end'],
                            'recordCount': count, 'recordBytes': 16})
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
