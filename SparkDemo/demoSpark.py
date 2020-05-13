from pyspark.sql import SparkSession
import os

# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"

spark = SparkSession.builder.appName('First_Demo').getOrCreate()

df = spark.read.csv('/root/PycharmProjects/SparkDemo/new_customers.csv', inferSchema='True', header='True')

ab = df.select('Names', 'Age', 'Years', 'Company').filter('Years < 8 and Age = 37.0').collect()

sc = spark.sparkContext.parallelize(ab)

print(sc.collect())
print(type(sc.collect()))
