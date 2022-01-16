#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
import os
os.environ['JAVA_HOME'] = 'C:\Java\jdk-11'  # 这里的路径为java的bin目录所在路径

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                                       "org.apache.kafka:kafka-clients:2.8.0") \
        .appName("pyspark_structured_streaming_kafka") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "testtopic2") \
        .load()

    words = df.selectExpr("CAST(value AS STRING)")

    schema = StructType() \
        .add("name", StringType()) \
        .add("age", StringType()) \
        .add("sex", StringType())

    # 通过from_json，定义schema来解析json
    res = words.select(from_json("value", schema).alias("data")).select("data.*")

    query = res.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
