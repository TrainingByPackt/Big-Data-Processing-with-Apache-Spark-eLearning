from datetime import datetime

import argparse
import random
import time
import socket

def get_line():
    """
    Returns a string with the current timestamp
    and a random event chosen from a list
    of events at random
    """
    random.seed(datetime.utcnow().microsecond)
    dt = datetime.utcnow()\
        .strftime('%Y-%m-%d %H:%M:%S.%f')
    event = random.choice(['event1', 'event2', 'event3'])
    return '{};{}\n'.format(dt, event).encode('utf-8')

def randomize_interval(interval):
    """
    Returns a random value sligthly different
    from the orinal interval parameter
    """
    random.seed(datetime.utcnow().microsecond)
    delta = interval + random.uniform(-0.1, 0.9)
    # delay can not be 0 or negative
    if delta <= 0:
        delta = interval
    return delta

def initialize(port=9876, interval=0.5):
    """
    Initialize a TCP server that returns a non deterministic
    flow of simulated events to its clients
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', port)
    sock.bind(server_address)
    sock.listen(5)
    print("Listening at {}".format(server_address))
    try:
        connection, client_address = sock.accept()
        print('connection from', client_address)
        while True:
            line = get_line()
            connection.sendall(line)
            time.sleep(randomize_interval(interval))
    except Exception as e:
        print(e)

    finally:
        sock.close()

def main():
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
