import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import *


class SparkConsumer:
    def __init__(self):
        self.localHost = 'localhost:9092'
        self.topicname = 'payments'
        self.startingOffsets = 'earliest'
        self.sparkModule = SparkSession.builder.appName("SparkKafkaConsumer").getOrCreate()
        pass

    def main_method(self):
        schema = self.schema()
        dataframe = self.spark_source()
        df = self.transformation(dataframe, schema)
        self.spark_sink(df)

    def schema(self):
        # Define schema of json
        jsonSchema = StructType([StructField("NM", StringType(), True),
                                 StructField("CDI", StringType(), True),
                                 StructField("AMT", StringType(), True),
                                 StructField("DAN", StringType(), True),
                                 StructField("DSN", StringType(), True),
                                 StructField("CAN", StringType(), True),
                                 StructField("CSN", StringType(), True),
                                 StructField("DT", StringType(), True)
                                 ])
        return jsonSchema

    def spark_source(self):
        lines = self.sparkModule \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.localHost) \
            .option("subscribe", self.topicname) \
            .option("startingOffsets", self.startingOffsets) \
            .load()

        lines = lines.selectExpr(
            "CAST(value AS STRING)"
        )
        return lines

    def transformation(self, lines, jsonSchema):

        # pis service
        def pis(x, y, z):
            debit = int(x)
            credit = int(y)
            if z == 'Credit':
                if credit % 2 == 0:
                    return 'Unisys'
                else:
                    return 'Sap'
                pass
            else:
                if debit % 2 == 0:
                    return 'Unisys'
                else:
                    return 'Sap'

        lines = lines.select("value",
                             from_json(lines["value"].cast("string"), jsonSchema).alias('person'))

        lines = lines.selectExpr("person.*")

        '''
        lines = lines.selectExpr("person.NM", "person.CDI", "person.AMT", "person.DAN", "person.DSN", "person.CAN", 
                                 "person.CSN", "person.DT")
                                 '''

        lines = lines.withColumnRenamed('NM', 'Name') \
            .withColumnRenamed('CDI', 'Indicator') \
            .withColumnRenamed('AMT', 'Amount') \
            .withColumnRenamed('DAN', 'DebitAccountNumber') \
            .withColumnRenamed('DSN', 'DebitSortCode') \
            .withColumnRenamed('CAN', 'CreditAccountNumber') \
            .withColumnRenamed('CSN', 'CreditSortCode') \
            .withColumnRenamed('DT', 'DateTime')
        lines = lines.dropna()

        pis_udf = udf(lambda x, y, z: pis(x, y, z), StringType())
        lines = lines.withColumn('Ledger', pis_udf(lines.DebitSortCode, lines.CreditSortCode, lines.Indicator))

        return lines

    def spark_sink(self, lines):
        query = lines \
            .writeStream \
            .format("console") \
            .option("truncate", False) \
            .start() \
            .awaitTermination()


test = SparkConsumer()
test.main_method()
