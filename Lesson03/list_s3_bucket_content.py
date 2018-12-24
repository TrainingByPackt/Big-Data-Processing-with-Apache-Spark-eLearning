import argparse
import os
import boto3


def list_bucket(Bucket):
    client = boto3.client('s3')
    data = client.list_objects_v2(Bucket=Bucket)
    result = []
    if 'Contents' in data:
        result = [r['Key'] for r in data['Contents']]
    return result


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
        '--Bucket', required=True, help='S3 Bucket Name'
    )

    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    """
    Creates an Amazon Kinesis stream using boto3 library
    """
    args, _ = parse_known_args()
    print(
        list_bucket(args.Bucket))


if __name__ == '__main__':
    main()
