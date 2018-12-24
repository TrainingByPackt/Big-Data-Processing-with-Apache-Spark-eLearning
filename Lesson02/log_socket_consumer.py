from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import argparse
import re
import os


def parse_log_entry(msg):
    """
    Parse a log entry from the format
    $ip_addr - [$time_local] "$request" $status $bytes_sent $http_user_agent"
    to a dictionary
    """
    data = {}

    # Regular expression that parses a log entry
    search_term = '(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+\-\s+\[(.*)]\s+'\
        '"(\/[/.a-zA-Z0-9-]+)"\s+(\d{3})\s+(\d+)\s+"(.*)"'

    values = re.findall(search_term, msg)

    if values:
        val = values[0]
        data['ip'] = val[0]
        data['date'] = val[1]
        data['path'] = val[2]
        data['status'] = val[3]
        data['bytes_sent'] = val[4]
        data['agent'] = val[5]
    return data

def aggregate_by_column(
        stream1, stream2, stream3, column_name='path'):

    def transform(record):
        return record.map(parse_log_entry).filter(lambda record: record)\
        .map(lambda record: (record[column_name], record))

    def clean(record):
        key = record[0]
        value = tuple(
            x for x in record[1][0] if x
        )
        return (key, value)

    # Join streams
    joined_stream = transform(stream1).leftOuterJoin(transform(stream2)).\
        leftOuterJoin(transform(stream3))
    # clean joined stream
    return joined_stream.map(clean)


def update_global_state(key_value_pairs):
    def update(new_values, accumulator):
        """
        Counts the number of requests by
        key
        """

        if accumulator is None:
            accumulator = 0

        if not new_values:
            # if not values, return 0 + accumulator
            _ = tuple([0])

        elif type(new_values) in (list, tuple,):
            # Assumes a list of lists or similar counts the number of items
            _ = tuple(len(r) for r in new_values if type(r) in (list, tuple,))
        else:
            _ = tuple(0)

        return sum(_, accumulator)

    return key_value_pairs.updateStateByKey(update)

def join_aggregation(stream1, stream2, stream3):
    """
    Step 1: Joins all streams and return a (key, record)
        dataset
    Step 2: counts all values by column_name
        dataset based on the path
    Step 3: Write updates in the file system.
    """
    path_aggregation = aggregate_by_column(
        stream1, stream2, stream3, column_name='path')
    global_counts = update_global_state(path_aggregation)
    data_dir = os.path.join(
        os.environ['SPARK_DATA'],'streams', 'count_by_request')
    global_counts.saveAsTextFiles(data_dir)

def consume_records(
        interval=1, host='localhost', port1=9876, port2=12345,
        port3=8765):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    sc, stream_context = initialize_context(interval=interval)
    stream1 = stream_context.socketTextStream(host, port1)
    stream2 = stream_context.socketTextStream(host, port2)
    stream3 = stream_context.socketTextStream(host, port3)
    join_aggregation(stream1, stream2, stream3)
    stream_context.start()
    stream_context.awaitTermination()


def initialize_context(interval=1, checkpointDirectory='/tmp'):
    """
    Creates a SparkContext, and a StreamingContext object.
    Initialize checkpointing
    """
    spark_context = SparkContext(appName='LogSocketConsumer')
    stream_context = StreamingContext(spark_context, interval)
    stream_context.checkpoint(checkpointDirectory)
    return spark_context, stream_context


def main():
    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)


    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--port1', required=False, default=9876, help='Port 1', type=int)

    parser.add_argument(
        '--port2', required=False, default=12345, help='Port 2', type=int)

    parser.add_argument(
        '--port3', required=False, default=8765, help='Port 3', type=int)


    parser.add_argument(
        '--host', required=False, default='localhost', help='Host')

    args, extra_params = parser.parse_known_args()

    consume_records(
        interval=args.interval, port1=args.port1, port2=args.port2,
        port3=args.port3,  host=args.host)


if __name__ == '__main__':
    main()
