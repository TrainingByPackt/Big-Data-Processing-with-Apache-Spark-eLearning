import os
import boto3


def list_buckets():
    """
    Executes the function S3.Client.list_buckets to get a list
    a of buckets by account
    """
    result = []
    client = boto3.client('s3')
    bucket_list = client.list_buckets()
    if 'Buckets' in bucket_list:
        result = [
         b['Name'] for b in bucket_list['Buckets']
        ]

    return result


def main():
    """
    List AWS S3 buckets using boto3 library
    """
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)

    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)
    for b in list_buckets():
        print(b)


if __name__ == '__main__':
    main()
