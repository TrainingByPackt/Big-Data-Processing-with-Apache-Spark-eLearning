import argparse
import os
import boto3


def create_stream(StreamName=None, ShardCount=1, region_name='us-west-2'):
    """
    Uses the function Kinesis.Client.create_stream
    to create a new Kinesis data stream.

    Each shard can support reads up to five transactions per second, up to a
    maximum data read total of 2 MB per second.

    Each shard can support writes up to 1,000 records per
    second, up to a maximum data write total of 1 MB per second.
    """
    assert StreamName is not None

    client = boto3.client('kinesis', region_name=region_name)
    stream = client.create_stream(
        StreamName=StreamName, ShardCount=ShardCount
    )
    return stream



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
    parser.add_argument('--StreamName', required=True, help='Stream name')
    parser.add_argument(
        '--ShardCount', required=True, help='Number of Shards', type=int)
    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    """
    Creates an Amazon Kinesis stream using boto3 library
    """
    args, _ = parse_known_args()
    print(
        create_stream(
            StreamName=args.StreamName,
            ShardCount=args.ShardCount, region_name=args.region_name))


if __name__ == '__main__':
    main()
