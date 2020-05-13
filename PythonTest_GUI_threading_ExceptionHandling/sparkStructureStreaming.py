from pyspark.sql import SparkSession
import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'

from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode, split


sparkModule = SparkSession.builder.appName("wordCountStreamingkafka").getOrCreate()

df = sparkModule\
  .readStream.\
  format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "qwerty")\
  .load()

ad = df.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(offset as STRING)")

words = ad.select(
   explode(
       split(ad.value, " ")
   ).alias("word")
)

aggDF = words.groupBy("word").count()
print("hello123")
df.printSchema

query = aggDF.writeStream \
  .outputMode("complete") \
  .queryName("t") \
  .format("memory") \
  .start()
sparkModule.sql("select * from t").show(truncate = False)
#query.awaitTermination()

#sparkModule.sql("select * from t").show(truncate = False)

