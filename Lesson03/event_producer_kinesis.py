from datetime import datetime

import argparse
import boto3
import json
import os
import random
import time
import uuid


def get_line():
    """
    Returns a string with the current timestamp and a random event chosen from
    a list of events at random
    """
    random.seed(datetime.utcnow().microsecond)
    dt = datetime.utcnow()\
        .strftime('%Y-%m-%d %H:%M:%S.%f')
    event = random.choice(['event1', 'event2', 'event3'])
    return '{};{}'.format(dt, event)


def randomize_interval(interval):
    """
    Returns a random value sligthly different
    from the original interval parameter
    """
    random.seed(datetime.utcnow().microsecond)
    delta = interval + random.uniform(-0.1, 0.9)
    # delay can not be 0 or negative
    if delta <= 0:
        delta = interval
    return delta


def initialize(StreamName=None, interval=1, region_name='us-west-2'):
    """
    Use Kinesis.Client.put_record function to send data to a kinesis
    stream
    """
    client = boto3.client('kinesis', region_name=region_name)
    while True:
        line = get_line()
        payload = {
            'value': line,
            'timestamp': str(datetime.utcnow()),
            'id': str(uuid.uuid4())
        }

        r = client.put_record(
            StreamName=StreamName,
            Data=json.dumps(payload),
            PartitionKey=str(uuid.uuid4())
        )
        print('Record stored in shard {}'.format(r['ShardId']))
        # Simulates a non deterministic delay
        time.sleep(randomize_interval(interval))


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
        '--interval', required=False, default=0.5,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')

    parser.add_argument('--StreamName', required=True, help='Stream name')

    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    """
    List Amazon Kinesis streams associated to a particular AWS account
    using boto3 library
    """
    args, extra_params = parse_known_args()
    initialize(
        StreamName=args.StreamName,
        interval=args.interval, region_name=args.region_name)


if __name__ == '__main__':
    main()
