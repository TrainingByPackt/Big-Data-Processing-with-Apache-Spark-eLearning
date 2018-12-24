from datetime import datetime

import argparse
import os
import boto3
import random
import time
import json
import uuid


HTTP_CODES = (
    200, 201, 300, 301, 302, 400, 401, 404, 500, 502
)


PATHS = (
    '/', '/home', '/login', '/user', '/user/profile',
    '/user/network/friends'
)


AGENTS = (
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
    'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko)'
    'Chrome/41.0.2224.3 Safari/537.36',
    'EsperanzaBot(+http://www.esperanza.to/bot/)',
    'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)'
)


PARTITION_KEYS = (
    'a574ddfa-72f6-47ba-8d47-07bade0e802e',
    '671d7649-1d24-46ea-bf16-d72500ea27bd',
    '06bbd823-0afb-4593-924e-7eddfc7071c7',
    'f5e44b34-5088-4249-92d5-3447af9c7062',
    'a7393aed-85ef-46d8-8a20-e1e21e61c9d7',
    '350724b2-2a68-42bb-ac2e-21b92b508fe1',
    '5ba81cc2-2d3c-4593-8b35-4ebd64959d20',
    'ef8b3d91-52ae-4349-b864-4f6f64a42660',
    'f0b8332a-d2d2-4db0-8403-435d13c1c14b',
    'f30c6c85-2721-4ac5-8fd9-0837e6f65f79'
)


def get_line():
    """
    Similates a log entry for a webserver in the format
    $remote_addr - [$time_local] "$request" $status $body_bytes_sent
    $http_user_agent"
    """
    base = '{ip} - [{time}] "{request}" {status} {bytes} "{agent}"'
    data = {}
    data['ip'] = '{0}.{1}.{2}.{3}'.format(
        random.randint(0, 255), random.randint(0, 255), random.randint(0, 255),
        random.randint(0, 255))

    data['time'] = datetime.utcnow()
    data['request'] = random.choice(PATHS)
    data['status'] = random.choice(HTTP_CODES)
    data['bytes'] = random.randint(1, 2048)
    data['agent'] = random.choice(AGENTS)
    return base.format(**data)


def generate_records(StreamName=None, delay=1, region_name='us-west-2'):
    assert StreamName is not None
    client = boto3.client('kinesis', region_name=region_name)
    while True:
        line = get_line()
        payload = {
            'value': line,
            'timestamp': str(datetime.utcnow()),
            'id': str(uuid.uuid4())
        }
        time.sleep(delay)
        r = client.put_record(
            StreamName=StreamName,
            Data=json.dumps(payload),
            PartitionKey=random.choice(PARTITION_KEYS)
        )
        print(r)


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
    generate_records(
        StreamName=args.StreamName,
        delay=args.delay, region_name=args.region_name)

if __name__ == '__main__':
    main()
