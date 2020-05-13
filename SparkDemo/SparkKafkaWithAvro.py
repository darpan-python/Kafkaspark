from pyspark.sql import SparkSession
import os
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import avro.datafile
import avro.io
import avro.schema
import io

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 pyspark-shell'
os.environ['PYSPARK_PYTHON'] ='python3.6'

schema = '/root/PycharmProjects/KafkaPycharm/Kafka/user.avsc'

def Decoder(msg):
    bytes_io = io.BytesIO(msg)
    decoder = avro.io.BinaryDecoder(bytes_io)
    reader = avro.io.DatumReader(schema)
    Msg= reader.read(decoder)
    return Msg


sc = SparkContext('local[*]','kafkaReceiver12')
ssc = StreamingContext(sc, 10)

ka = KafkaUtils.createDirectStream(ssc, ['newTopic11234456'],
                                   {'metadata.broker.list':'localhost:9092', 'auto.offset.reset': 'smallest'},
                                   valueDecoder='utf-8')

ka.pprint()

ssc.start()
ssc.awaitTermination()