import socket
import sys
from datetime import datetime

# sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# server_address = ('localhost', 9999)
# print('starting up on {} port {}'.format(*server_address))
# sock.bind(server_address)
#
# while True:
#     print('\nwaiting to receive message')
#     data, address = sock.recvfrom(4096)
#     csv_group=data.decode("utf-8").split('\n')
#     for csv in csv_group:
#         print(csv)

    # print('received {} bytes from {}'.format(len(data), address))
    # csv_group_arr=data.decode("utf-8").split('\n')
    # for i in range(0, 5):
    #     csv_arr=csv_group_arr[i].split(',')
    #     print("unix_time: %s, dOctets: %s" % (datetime.fromtimestamp(int(int(csv_arr[3])/1000)), csv_arr[15]))

# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# import time

# def get_tstamp_doctects(line):
#     line_arr = line.split(',')
#     tstamp = str(datetime.fromtimestamp(int(int(line_arr[3])/1000)))[:-3]
#     doct = int(line_arr[15])
#     return (tstamp, (doct ,doct, 1))
#
# sc = SparkContext("local[*]", "StreamProcess")
# ssc = StreamingContext(sc, 60)
# lines = ssc.socketTextStream("localhost", 9998)
# tuples = lines.map(lambda line:get_tstamp_doctects(line))
# results = tuples.reduceByKey(lambda x, y:(max(x[0], y[0]), x[1]+y[1], x[2]+y[2]))
# results.pprint(31)
#results.map(lambda result:(result[0], result[1][0], result[1][1]/result[1][2])).repartition(1).saveAsTextFiles("/Users/dastonzerg/Google Drive/scu/coursework/2018_2019_spring/coen242/assignments/outputs/output")
#results.repartition(1).saveAsTextFiles("/Users/dastonzerg/Google Drive/scu/coursework/2018_2019_spring/coen242/assignments/outputs/output")

# words = lines.flatMap(lambda line: line.split("\n"))
# pairs = words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKey(lambda x, y: x + y)
#lines.repartition(1).saveAsTextFiles("/Users/dastonzerg/Google Drive/scu/coursework/2018_2019_spring/coen242/assignments/outputs/output")
# ssc.start()
# ssc.awaitTermination()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql import functions as F
from pyspark.sql.functions import split, window, substring, from_unixtime

spark_session = SparkSession \
    .builder.master("local[*]") \
    .appName("StreamProcess") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", 1000)\
    .config("spark.sql.files.maxPartitionBytes", 134217728) \
    .getOrCreate()

spark_df = spark_session \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 1235) \
    .load()

spark_df = spark_df.select(spark_df.value.alias("csv_line"))
spark_df = spark_df.select(split(spark_df.csv_line, ",").alias("csv_arr"))
spark_df = spark_df.select(from_unixtime(substring(spark_df.csv_arr[3], 1, 10)).alias("tstamp"),
                          spark_df.csv_arr[15].alias("doctets"))
result_df = spark_df.groupBy(window("tstamp", "1 minute", "1 minute")) \
   .agg(F.max(spark_df.doctets), F.avg(spark_df.doctets))

query = result_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false")\
    .option("numRows", "30") \
    .start() \
    .awaitTermination()
