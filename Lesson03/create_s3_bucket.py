import argparse
import os
import boto3


def create_bucket(Bucket):
    """
    Executes the function S3.ServiceResource.create_bucket to create
    a new S3 bucket
    """
    client = boto3.client('s3')
    return client.create_bucket(Bucket=Bucket)


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
    Creates an Amazon S3 Bucket  using boto3 library
    """
    args, _ = parse_known_args()
    print(
        create_bucket(args.Bucket))


if __name__ == '__main__':
    main()
