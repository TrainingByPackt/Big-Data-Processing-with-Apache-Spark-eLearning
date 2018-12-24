from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from spark_recommender import MovieRecommender
from datetime import datetime
import argparse


def parse_rating(msg):
    """
    Parse every movie rating
    """

    values = msg.split(':')

    return {
        'userId': int(values[0]),
        'movieId': int(values[1]),
        'rating': float(values[2]),
        'timestamp':  datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    }


def update_model(rdd, recommender):
    """
    Update recommender model with ratings collected from live stream
    """
    print('updating recommender model')
    recommender.update_model(rdd.collect())
    print('total ratings: {}'.format(recommender.ratings.count()))


def consume_records(
        interval=1, windowLength=4, slideInterval=2,
        port=9876, host='localhost'):
    """
    Collects movie ratings from users and updates a machine learning
    model for movies recommendations. Because recompute predictions
    models is expensie. This Stream use windowing operations to send
    batches of data for the model update instead of recomputing
    the model everytime that a new RDD arrives.
    """
    spark_context = SparkContext(appName='LogSocketConsumer')
    spark_context.setLogLevel("DEBUG")
    sql_context = SQLContext(spark_context)
    recommender = MovieRecommender(spark_context, sql_context)
    stream_context = StreamingContext(spark_context, interval)
    stream = stream_context.socketTextStream(host, port)

    ratings = stream.window(windowLength, slideInterval).map(parse_rating)
    ratings.foreachRDD(lambda rdd: update_model(rdd, recommender))

    stream_context.start()
    stream_context.awaitTermination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--interval', required=False, default=2.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--windowLength', required=False, default=4,
        help='Window Length', type=float)

    parser.add_argument(
        '--slideInterval', required=False, default=2,
        help='slideInterval', type=float)

    parser.add_argument(
        '--port', required=False, default=9876,
        help='Port', type=int)

    parser.add_argument(
        '--host', required=False, default='localhost', help='Host')

    args, extra_params = parser.parse_known_args()
    consume_records(
        interval=args.interval, windowLength=args.windowLength,
        slideInterval=args.slideInterval, port=args.port, host=args.host)


if __name__ == '__main__':
    main()
