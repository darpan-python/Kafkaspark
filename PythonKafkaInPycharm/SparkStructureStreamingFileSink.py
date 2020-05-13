import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, Column
from pyspark.sql.functions import from_json
from pyspark.sql import functions as F
from pyspark.sql.types import *

sparkModule = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()

# Define schema of json
jsonSchema = StructType([StructField("name", StringType(), True),
                         StructField("favorite_color", StringType(), True),
                         StructField("favorite_number", StringType(), True)
                         ])

lines = sparkModule \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "acvbn") \
    .option("startingOffsets", "earliest") \
    .load()

lines = lines.selectExpr(
    "CAST(value AS STRING)"
)

lines = lines.select("value", from_json(lines["value"].cast("string"), jsonSchema).alias('person'))
lines = lines.selectExpr("person.name", "person.favorite_color", "person.favorite_number")

'''
words = lines.select(
   explode(
       split(lines.value, ",")
   ).alias("word")
)
'''

query = lines \
    .writeStream \
    .outputMode("complete") \
    .format("CSV") \
    .option("path", "/home/darpan/snap/Test") \
    .option("checkpointLocation", "/home/darpan/snap") \
    .start() \
    .awaitTermination()

'''
query = lines \
    .writeStream \
    .format("memory") \
    .queryName("myTable_json") \
    .start()

sparkModule.sql("select parsed_value from myTable_json ").show(15, False)

'''