import socket
import selectors
from time import sleep
from pyspark.sql.functions import split, explode, window
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
import os
# Path for spark source folder
os.environ['SPARK_HOME'] = "/home/darpan/spark-2.4.0-bin-hadoop2.7"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = 'python3.6'
from pyspark.sql import SparkSession

def socketIntializer(host, post):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        return s
    except Exception as msg:
        print("connection is still open " + str(msg))
        s.close()

def preparingsocket(s, timesleep = 1):
    try:
        sleep(timesleep)
        s.bind(('', port))
        print("socket binded")
        # put the socket into listening mode
        s.listen(2)
        print("socket is listening")
        c, addr = s.accept()
        return [c, addr]
    except Exception as e:
        # Close the connection with the client
        print("Error occurred while creating an error " + str(e))
        s.close()
        c.close()

def sendingdataToSocket(c):
    while True:
        try:
            msg1 = "Hello"
            # send a thank you message to the client.
            c.send(msg1.encode())
            print(" Received message is '{}'".format(msg1))
            msg = c.recv(1024).decode()
            print(" Received message is '{}'".format(msg))
        finally:
            # Close the connection with the client
            print("Message sent ")
            c.close()
            break

def readingfromsocket(s: socket):
    try:
        s.connect((host, port))
        while True:
            data = s.recv(255)
            print(data)
    except Exception as e:
        print("error occurred " + str(e))
        pass
    finally:
        s.close()

def readingdataFromCSV(timesleep = 1):
    import csv

    s = preparingsocket(socketIntializer(host, port), timesleep)[0]
    with open(r"/home/darpan/spark-2.4.0-bin-hadoop2.7/python/02/demos/datasets/player.csv", 'r') as CSVFile:
        csvreader = csv.reader(CSVFile)
        linecount = 0
        for row in CSVFile:
            if linecount == 0:
                linecount = linecount + 1
                continue
            else:
                test = str(map(lambda x: str.join(x), row))
                s.send(str(row).encode())
                sleep(1)
                print(linecount)
                linecount = linecount + 1
                pass

def sparkrundf():

    spark = SparkSession.builder.appName("Socket Streaming").getOrCreate()

    # Default Log Level change using some configuration.
    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    spark.sparkContext.setLogLevel("Error")

    # Spark streaming for player streaming through socket streaming.
    df = spark.readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 12345)\
        .load()

    split_col = split(df['value'], ',')

    for idx, field in enumerate(customschema()):
        df = df.withColumn(field.name, split_col.getItem(idx).cast(field.dataType))

    df = df.drop(df["value"])

    sparkreadcsv = spark.read\
        .csv(r"/home/darpan/spark-2.4.0-bin-hadoop2.7/python/02/demos/datasets/player_attributes.csv", header="True", inferSchema="True")

    df = df.join(sparkreadcsv, df['Player_Fifa_Id'] == sparkreadcsv["player_fifa_api_id"])


    # df = df.groupBy(window(df["Birthday"], "30 days").alias("window12"))\
    #     .agg({"Height": "max"}).alias('max_height')
    #
    # df = df.select(
    #     df["window12"].getField("start").alias("start"),
    #     df["window12"].getField("end").alias("end"),
    #     df["max(Height)"].alias("max_height"))
    #
    # df = df.orderBy(df["start"], df["end"], df["max_height"])

    #df = df.withColumn("Start", df["window12"].getField("start").cast(StringType()))
    #     .withColumn("End", df["window"].getItem(1).cast(StringType()))\
    #     .select("Start", "End", "max_height")

    df.writeStream\
        .format("console")\
        .outputMode("append")\
        .trigger(processingTime = "1 seconds")\
        .start()\
        .awaitTermination()

def customschema():

    column = StructType([StructField("Id", IntegerType(), False),
                          StructField("PlayerId", IntegerType(), False),
                          StructField("PlayerName", StringType(), False),
                          StructField("Player_Fifa_Id", IntegerType(), False),
                          StructField("Birthday", StringType(), False),
                          StructField("Height", IntegerType(), False),
                          StructField("Weight", IntegerType(), False),
                         ])
    return column



if __name__ == "__main__":
    import threading

    sel = selectors.DefaultSelector()
    host = 'localhost'
    port = 12345
    timesleep = 5

    # Used Threading concept to run socket and spark both.
    threading.Thread(target = readingdataFromCSV, args = (timesleep,)).start()
    sparkrundf()

    #sendingdataToSocket(preparingsocket(socketIntializer(host, port))[0])

    #readingfromsocket(socketIntializer(host, port))

    #readingdataFromCSV()

