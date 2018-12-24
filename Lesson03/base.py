from pyspark import SparkContext
from pyspark.streaming import StreamingContext

StreamName = ''
endpoint = ''
stream_context = ''
region_name = ''
stream = ''
interval = ''


from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

stream = KinesisUtils.createStream(
    stream_context, 'EventLKinesisConsumer', StreamName, endpoint,
    region_name, InitialPositionInStream.LATEST, interval)
