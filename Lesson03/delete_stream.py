import argparse
import os
import boto3


def delete_stream(StreamName=None, region_name='us-west-2'):
    """
    Executes Kinesis.Client.delete_stream function
    """
    assert StreamName is not None

    client = boto3.client('kinesis', region_name=region_name)
    return client.delete_stream(
        StreamName=StreamName
    )


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
    args, extra_params = parser.parse_known_args()
    return args, extra_params


def main():
    """
    Deletes an Amazon Kinesis stream using boto3 library
    """

    args, _ = parse_known_args()
    print(
        delete_stream(
            StreamName=args.StreamName, region_name=args.region_name))


if __name__ == '__main__':
    main()
