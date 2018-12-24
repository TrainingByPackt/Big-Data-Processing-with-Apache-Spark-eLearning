#  E.2.1


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import random
key_value_pairs = ''
sc = ''
parse_entry = ''

spark_context = SparkContext(appName='TCPSocketConsumer')
stream_context = StreamingContext(spark_context, 10)
stream_context.checkpoint('/tmp')


stream = stream_context.socketTextStream('localhost', 98765)
stream2 = stream_context.socketTextStream('localhost', 12345)


def update_global_event_counts(key_value_pairs):
    """
    Function that receives as parameter a Dstream
    and applies an update function in order to keep
    aggregated information about event types counts.
    """
    def update(new_values, accumulator):
        if accumulator is None:
            accumulator = 0
        return sum(new_values, accumulator)

    return key_value_pairs.updateStateByKey(update)



key_value_pairs = stream.map(parse_entry).map(lambda record: (record['event'], 1))
key_value_pairs2 = stream2.map(parse_entry).map(lambda record: (record['event'], 1))



running_event_counts = update_global_event_counts(key_value_pairs)
running_event_counts2 = update_global_event_counts(key_value_pairs2)
running_event_counts.pprint()
running_event_counts2.pprint()


n_counts_joined = running_event_counts.leftOuterJoin(running_event_counts2)
n_counts_joined.pprint()

def aggregate_joined_stream(pair):
    key = pair[0]
    values = [val for val in pair[1] if val is not None]
    return(key, sum(values))

n_counts_joined.map(aggregate_joined_stream).pprint()



running_event_counts.pprint()

other_source = sc.parallelize([
    ('event1', random.randint(0, 10)),
    ('event2', random.randint(0, 10)),
])
n_pair = key_value_pairs.transform(lambda rdd: rdd.join(other_source))
