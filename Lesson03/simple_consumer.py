import argparse
import time
import os
import boto3
import json


def read_records(StreamName=None, delay=1, region_name='us-west-2'):
    assert StreamName is not None
    client = boto3.client('kinesis', region_name=region_name)
    response = client.describe_stream(StreamName=StreamName)
    shard_ids = [
        s['ShardId'] for s in response['StreamDescription']['Shards']]

    def get_iterators():
        shard_iters = []
        for sid in shard_ids:
            result = client.get_shard_iterator(
                StreamName=StreamName, ShardId=sid,
                ShardIteratorType='LATEST')

            if 'ShardIterator' in result:
                shard_iters.append(result['ShardIterator'])

        return shard_iters

    while True:
        print('outer loop')

        for shard_iterator in get_iterators():
            print('inner loop')
            record_response = client.get_records(
                ShardIterator=shard_iterator, Limit=1)

            if record_response['Records']:
                data = json.loads(
                    record_response['Records'][0]['Data'].decode('utf-8'))
                print(data)

            time.sleep(delay)
        time.sleep(delay)


def main():
    """
    List Amazon Kinesis streams associated to a particular AWS account
    using boto3 library
    """

    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)

    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')
    parser.add_argument('--StreamName', required=True, help='Stream name')
    parser.add_argument(
        '--delay', required=False, default=1,
        help='Delay in seconds between records', type=int)

    args, extra_params = parser.parse_known_args()
    read_records(
        StreamName=args.StreamName,
        delay=args.delay, region_name=args.region_name)

if __name__ == '__main__':
    main()
