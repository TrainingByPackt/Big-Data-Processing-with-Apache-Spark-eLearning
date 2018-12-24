from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from datetime import datetime

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
    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

    return key_value_pairs.updateStateByKey(updateFunction)


def consume_records(
        interval=1, host='localhost', port=9876):
    """
    Create a local StreamingContext with two working
    thread and batch interval of 1 second
    """
    spark_context = SparkContext(appName='LogSocketConsumer')
    stream_context = StreamingContext(spark_context, interval)
    stream = stream_context.socketTextStream(host, port)

    # counts number of events
    event_counts = aggregate_by_event_type(stream)
    event_counts.pprint()
    key_value_pairs = stream.map(parse_entry)\
        .map(lambda record: (record['event'], 1))
    running_event_counts = update_global_event_counts(key_value_pairs)
    running_event_counts.pprint()
    stream_context.start()
    stream_context.awaitTermination()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--port', required=False, default=9876,
        help='Port', type=int)

    parser.add_argument(
        '--host', required=False, default='localhost', help='Host')

    args, extra_params = parser.parse_known_args()
    consume_records(
        interval=args.interval, port=args.port, host=args.host)


if __name__ == '__main__':
    main()
