from pyspark.sql import SparkSession
import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 pyspark-shell'
os.environ['PYSPARK_PYTHON'] ='python3.6'
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils



sc = SparkContext('local[*]','kafkaReceiver12')
ssc = StreamingContext(sc,10)

ka = KafkaUtils.createDirectStream(ssc,['newTopic12'],{'metadata.broker.list':'localhost:9092'})
#stream = ka.map(lambda x: x.split(' '))

#counts = stream.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
ka.pprint()

ssc.start()
ssc.awaitTermination()
