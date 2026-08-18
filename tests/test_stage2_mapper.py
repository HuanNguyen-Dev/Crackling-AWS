import importlib.util
import io
import json
import os
import struct
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class FakeClientError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


def load_lambda(name, relative_path, environment):
    fake_boto3 = types.ModuleType('boto3')
    fake_boto3.client = lambda service: object()
    fake_exceptions = types.ModuleType('botocore.exceptions')
    fake_exceptions.ClientError = FakeClientError
    fake_botocore = types.ModuleType('botocore')
    fake_botocore.exceptions = fake_exceptions

    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    with (
        patch.dict(os.environ, environment, clear=False),
        patch.dict(sys.modules, {
            'boto3': fake_boto3,
            'botocore': fake_botocore,
            'botocore.exceptions': fake_exceptions,
        }),
    ):
        spec.loader.exec_module(module)
    return module


class RangeS3:
    def __init__(self, data):
        self.data = data

    def get_object(self, Bucket, Key, Range):
        start_text, end_text = Range.removeprefix('bytes=').split('-')
        start = int(start_text)
        end = int(end_text) + 1
        return {'Body': io.BytesIO(self.data[start:end])}


class Stage2MapperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mapper = load_lambda(
            'stage2_mapper',
            'modules/isslMapper/lambda_function.py',
            {},
        )
        cls.dispatcher = load_lambda(
            'stage2_dispatcher',
            'modules/isslDispatcher/lambda_function.py',
            {'BUCKET': 'bucket', 'MAPPER_QUEUE': 'queue'},
        )

    def test_dispatcher_creates_deterministic_reducer_ready_task(self):
        guide = {'JobID': 'job-1', 'TargetID': 7, 'Sequence': 'A' * 23}
        manifest = {
            'issl': {'bucket': 'bucket', 'key': 'genome/issl/genome.issl'},
        }
        shard = {
            'shardId': 2,
            'startSlice': 4,
            'endSlice': 6,
            'startByte': 100,
            'endByte': 200,
        }

        first = self.dispatcher._mapper_task(
            guide, 'genome', 'genome/issl/shards.json', manifest, shard,
        )
        second = self.dispatcher._mapper_task(
            guide, 'genome', 'genome/issl/shards.json', manifest, shard,
        )

        self.assertEqual(first['taskId'], second['taskId'])
        self.assertEqual(first['shardId'], 2)
        self.assertTrue(first['output']['mitKey'].endswith('/mit.bin'))
        self.assertTrue(first['output']['cfdKey'].endswith('/cfd.bin'))

    def test_sparse_materialization_downloads_only_prefix_and_shard(self):
        source = bytes(range(100))
        self.mapper.s3_client = RangeS3(source)
        task = {
            'issl': {'bucket': 'bucket', 'key': 'index'},
            'startByte': 70,
            'endByte': 90,
        }
        manifest = {'layout': {'baseOffsetBytes': 30}}

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'index.issl')
            prefix_bytes, shard_bytes = self.mapper._materialize_sparse_issl(
                task, manifest, path,
            )
            with open(path, 'rb') as materialized:
                data = materialized.read()

        self.assertEqual(prefix_bytes, 30)
        self.assertEqual(shard_bytes, 20)
        self.assertEqual(data[:30], source[:30])
        self.assertEqual(data[30:70], bytes(40))
        self.assertEqual(data[70:90], source[70:90])

    def test_combined_records_are_split_and_zeroes_omitted(self):
        records = b''.join([
            self.mapper.MAPPER_RESULT.pack(1, 10, 1.5, 0.0),
            self.mapper.MAPPER_RESULT.pack(1, 11, 0.0, 2.5),
            self.mapper.MAPPER_RESULT.pack(1, 0xFFFFFFFF, 0.0, 0.0),
        ])

        with tempfile.TemporaryDirectory() as directory:
            combined = os.path.join(directory, 'combined.bin')
            mit = os.path.join(directory, 'mit.bin')
            cfd = os.path.join(directory, 'cfd.bin')
            with open(combined, 'wb') as output:
                output.write(records)

            counts = self.mapper._split_results(combined, mit, cfd)
            with open(mit, 'rb') as result:
                mit_record = result.read()
            with open(cfd, 'rb') as result:
                cfd_record = result.read()

        self.assertEqual(counts, (3, 1, 1))
        self.assertEqual(struct.unpack('<Id', mit_record), (10, 1.5))
        self.assertEqual(struct.unpack('<Id', cfd_record), (11, 2.5))


if __name__ == '__main__':
    unittest.main()
