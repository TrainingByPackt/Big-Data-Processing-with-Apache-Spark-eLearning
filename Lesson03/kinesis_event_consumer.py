from datetime import datetime
from glob import glob
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import argparse
import boto3
import json
import os


def parse_entry(record):
    """
    Event TCP sends sends data in the format
    timestamp:event\n
    """
    msg = record['value']
    values = msg.split(';')

    # remove \n character if exists

    event = values[1].replace('\n', '')

    return {
        'dt': datetime.strptime(
            values[0], '%Y-%m-%d %H:%M:%S.%f'),
        'event': event,
        'timestamp': datetime.strptime(
            record['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
    }


def update_global_event_counts(key_value_pairs):
    def update(new_values, accumulator):
        if accumulator is None:
            accumulator = 0
        return sum(new_values, accumulator)

    return key_value_pairs.updateStateByKey(update)

def aggregate_by_event_type(record):
    """
    Step 1. Maps every entry to a dictionary.
    Step 2. Transform the dataset in a set of
        tuples (event, 1)
    Step 3: Applies a reduction by event type
        to count the number of events by type
        in a given interval of time.
    """
    return record\
        .map(lambda x: json.loads(x))\
        .map(parse_entry)\
        .map(lambda record: (record['event'], 1))\
        .reduceByKey(lambda a, b: a+b)

def send_record(rdd, Bucket):
    """
    If rdd size is greater than 0, store the
    data as text in S3
    """
    if rdd.count() > 0:
        client = boto3.client('s3')
        data_dir = os.path.join(
            os.environ['SPARK_DATA'],
            'streams', 'kinesis_{}'.format(datetime.utcnow().timestamp()))
        rdd.saveAsTextFile(data_dir)
        for fname in glob('{}/part-0000*'.format(data_dir)):
            client.upload_file(fname, Bucket, fname)


def consume_records(
        interval=1, StreamName=None, region_name='us-west-2', Bucket=None):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    assert StreamName is not None

    endpoint = 'https://kinesis.{}.amazonaws.com/'.format(region_name)

    sc, stream_context = initialize_context(interval=interval)
    sc.setLogLevel("INFO")
    stream = KinesisUtils.createStream(
        stream_context, 'EventLKinesisConsumer', StreamName, endpoint,
        region_name, InitialPositionInStream.LATEST, interval)

    # counts number of events
    event_counts = aggregate_by_event_type(stream)
    global_counts = update_global_event_counts(event_counts)
    global_counts.pprint()
    # Sends data to S3
    global_counts.foreachRDD(lambda rdd: send_record(rdd, Bucket))
    stream_context.start()
    stream_context.awaitTermination()


def initialize_context(interval=1, checkpointDirectory='/tmp'):
    """
    Creates a SparkContext, and a StreamingContext object.
    Initialize checkpointing
    """
    spark_context = SparkContext(appName='EventLKinesisConsumer')
    stream_context = StreamingContext(spark_context, interval)
    stream_context.checkpoint(checkpointDirectory)
    return spark_context, stream_context


def parse_known_args():
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)
    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)

    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')

    parser.add_argument('--StreamName', required=True, help='Stream name')

    parser.add_argument(
        '--Bucket', required=True, help='S3 Bucket Name'
    )

    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    args, extra_params = parse_known_args()
    consume_records(
        interval=args.interval, StreamName=args.StreamName,
        Bucket=args.Bucket, region_name=args.region_name)


if __name__ == '__main__':
    main()
