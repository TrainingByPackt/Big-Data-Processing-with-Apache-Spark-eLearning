from datetime import datetime

import argparse
import random
import time
import socket
import os
import csv

users = []
movies = []


def load_data():
    global users
    global movies
    fname = os.path.join(
        os.environ['SPARK_DATA'], 'ratings.csv'
    )
    with open(fname) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)
        for row in reader:
            users.append(row[0])
            movies.append(row[1])


def get_line():
    """
    Returns a user:movie:rating pair
    """
    global users
    global movies
    random.seed(datetime.utcnow().microsecond)
    user = random.choice(users)
    movie = random.choice(movies)
    rating = random.choice([1, 2, 3, 4, 5])
    line = '{}:{}:{}\n'.format(user, movie, rating)
    print(line)
    return line.encode('utf-8')


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
    load_data()
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
    if 'SPARK_DATA' not in os.environ:
        print('SPARK_DATA directory or variable not set')
        exit(1)

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
