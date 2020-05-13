from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
from pyspark.sql import SparkSession
import os
# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON'] = 'python3.6'

from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode, split
from pyspark.sql.types import (StringType, StructField, IntegerType, StructType)

class KafkaConsumer:

    def __init__(self):
        self.spark = SparkSession.builder.appName('SparkDataFrame').getOrCreate()
        data_schema = [StructField("name", IntegerType(), False),
                       StructField("favorite_color", StringType(), False),
                       StructField("favorite_number", IntegerType(), False)]
        self.final_struc = StructType(fields=data_schema)


    def ConsumerLoad(self):

        # To consume messages

        consumer = KafkaConsumer('sample1'
                                 , bootstrap_servers='localhost:9092')
                                 #auto_offset_reset='latest',
                                 #enable_auto_commit=True,
                                 #group_id='my-group')

        schema_path = "/root/PycharmProjects/KafkaPycharm/Kafka/user.avsc"
        schema = avro.schema.parse(open(schema_path).read())

        for msg in consumer:
            bytes_reader = io.BytesIO(msg.value)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            data = reader.read(decoder)
            self.sparkData(data)

    def sparkData(self, data):
        dataframe = self.spark.read.json(data, schema=self.final_struc)
        dataframe.show()
        pass

test = KafkaConsumer()
test.ConsumerLoad()
