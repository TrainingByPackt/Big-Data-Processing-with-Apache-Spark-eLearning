import argparse
import os
import boto3
import json


def list_streams(region_name='us-west-2', Limit=10):
    """
    Executes Kinesis.Client.describe_stream_summary function
    """
    client = boto3.client('kinesis', region_name=region_name)
    result = client.list_streams(Limit=Limit)
    streams = []

    if 'StreamNames' in result:
        for st in result['StreamNames']:
            record = {}
            details = client.describe_stream_summary(
                StreamName=st)['StreamDescriptionSummary']
            record['StreamName'] = st
            record['StreamARN'] = details['StreamARN']
            record['OpenShardCount'] = details['OpenShardCount']
            shards_info = client.list_shards(StreamName=st)
            record['shards'] = shards_info['Shards']
            streams.append(record)
    return streams


def parse_known_args():
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
    parser.add_argument(
        '--Limit', required=False, default=100,
        help='Limit of records returned', type=int)
    args, extra_params = parser.parse_known_args()
    return args, extra_params


def main():
    """
    List all AWS Kinesis streams associated to a given AWS account
    using boto3 library
    """
    args, _ = parse_known_args()
    result = list_streams(
        region_name=args.region_name, Limit=args.Limit)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
