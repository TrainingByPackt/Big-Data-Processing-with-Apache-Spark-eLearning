from datetime import datetime

import argparse
import random
import time
import socket

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

def get_data():
    """
    Similates a log entry for a webserver in the format
    $remote_addr - [$time_local] "$request" $status $body_bytes_sent
    $http_user_agent"
    """
    random.seed(datetime.utcnow().microsecond)
    data = {}
    data['ip'] = '{0}.{1}.{2}.{3}'.format(
        random.randint(0, 255), random.randint(0, 255), random.randint(0, 255),
        random.randint(0, 255))

    data['time'] = datetime.utcnow()
    data['request'] = random.choice(PATHS)
    data['status'] = random.choice(HTTP_CODES)
    data['bytes'] = random.randint(1, 2048)
    data['agent'] = random.choice(AGENTS)
    return data

def get_line():
    base = '{ip} - [{time}] "{request}" {status} {bytes} "{agent}"\n'
    data = get_data()
    return base.format(**data)


def initialize(port=9876, interval=0.5):
    """
    Establish a connection with a stream socket consumer
    and push simulated log entries.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', port)
    print("Listening at {}".format(server_address))
    sock.bind(server_address)
    sock.listen(5)
    try:
        connection, client_address = sock.accept()
        print('connection from', client_address)
        while True:
            line = get_line().encode('utf-8')
            connection.sendall(line)
            print(line)
            time.sleep(interval)
    except Exception as e:
        print(e)

    finally:
        sock.close()


def main():
    """
    Push data to a TCP socket
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--port', required=False, default=9876,
        help='Port', type=int)

    parser.add_argument(
        '--interval', required=False, default=0.5,
        help='Interval in seconds', type=float)

    args, extra_params = parser.parse_known_args()
    initialize(port=args.port, interval=args.interval)


if __name__ == '__main__':
    main()
