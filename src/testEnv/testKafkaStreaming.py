# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

SparkJar = "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//commons-logging-1.1.3.jar," \
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//commons-pool2-2.6.2.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//hadoop-client-api-3.3.1.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//hadoop-client-runtime-3.3.1.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//htrace-core4-4.1.0-incubating.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//jsr305-3.0.0.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//kafka-clients-2.8.0.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//lz4-java-1.7.1.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//scala-library-2.12.15.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//slf4j-api-1.7.30.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//snappy-java-1.1.8.1.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-sql-kafka-0-10_2.12-3.2.0.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-tags_2.12-3.2.0.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-token-provider-kafka-0-10_2.12-3.2.0.jar,"\
    "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//unused-1.0.0.jar," \
    "file:///C://spark-3.2.0-bin-hadoop3.2//jars//spark-sql_2.12-3.2.0.jar," \
    "file:///C://spark-3.2.0-bin-hadoop3.2//jars//spark-streaming_2.12-3.2.0.jar"

spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars",SparkJar) \
        .config("spark.executor.extraClassPath", SparkJar) \
        .config("spark.executor.extraLibrary", SparkJar) \
        .config("spark.driver.extraClassPath", SparkJar) \
        .getOrCreate()
# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9999") \
  .option("subscribe", "testtopic") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.writeStream.start()
print(df.collect())

# # Subscribe to 1 topic, with headers
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:2181") \
#   .option("subscribe", "test1") \
#   .option("includeHeaders", "true") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
#
# # Subscribe to multiple topics
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:2181") \
#   .option("subscribe", "topic1,topic2") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# # Subscribe to a pattern
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:2181") \
#   .option("subscribePattern", "topic.*") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")