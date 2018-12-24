from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


import argparse


def parse_entry(msg):
    """
    Event TCP sends sends data in the format
    timestamp:event\n
    """
    values = msg.split(';')
    return {
        'dt': datetime.strptime(
            values[0], '%Y-%m-%d %H:%M:%S.%f'),
        'event': values[1]
    }


def aggregate_by_event_type(record):
    """
    Step 1. Maps every entry to a dictionary.
    Step 2. Transform the dataset in a set of
        tuples (event, 1)
    Step 3: Applies a reduction by event type
        to count the number of events by type
        in a given interval of time.
    """
    return record.map(parse_entry)\
        .map(lambda record: (record['event'], 1))\
        .reduceByKey(lambda a, b: a+b)

def update_global_event_counts(key_value_pairs):
    def update(new_values, accumulator):
        if accumulator is None:
            accumulator = 0
        return sum(new_values, accumulator)

    return key_value_pairs.updateStateByKey(update)


def aggregate_joined_stream(pair):
    key = pair[0]
    values = [val for val in pair[1] if val is not None]
    return(key, sum(values))


def join_aggregation(stream1, stream2):
    key_value_pairs = stream1.map(parse_entry)\
        .map(lambda record: (record['event'], 1))
    running_event_counts = update_global_event_counts(key_value_pairs)
    running_event_counts.pprint()

    key_value_pairs2 = stream2.map(parse_entry)\
        .map(lambda record: (record['event'], 1))
    running_event_counts2 = update_global_event_counts(key_value_pairs2)
    running_event_counts2.pprint()

    n_counts_joined = running_event_counts.leftOuterJoin(running_event_counts2)
    n_counts_joined.pprint()
    n_counts_joined.map(aggregate_joined_stream).pprint()

def consume_records(
        interval=1, host='localhost', port1=9876, port2=12345):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    sc, stream_context = initialize_context(interval=interval)
    stream1 = stream_context.socketTextStream(host, port1)
    stream2 = stream_context.socketTextStream(host, port2)
    join_aggregation(stream1, stream2)
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--port1', required=False, default=9876,
        help='Port', type=int)

    parser.add_argument(
        '--port2', required=False, default=12345,
        help='Port', type=int)

    parser.add_argument(
        '--host', required=False, default='localhost', help='Host')

    args, extra_params = parser.parse_known_args()
    consume_records(
        interval=args.interval, port1=args.port1,
        port2=args.port2, host=args.host)


if __name__ == '__main__':
    main()
