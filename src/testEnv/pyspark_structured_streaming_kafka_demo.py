# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import redis

KAFKA_INPUT_TOPIC_NAME_CONS = "tradeDetail"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    SparkJar = "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//commons-logging-1.1.3.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//commons-pool2-2.6.2.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//hadoop-client-api-3.3.1.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//hadoop-client-runtime-3.3.1.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//htrace-core4-4.1.0-incubating.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//jsr305-3.0.0.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//kafka-clients-2.8.0.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//lz4-java-1.7.1.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//scala-library-2.12.15.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//slf4j-api-1.7.30.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//snappy-java-1.1.8.1.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-sql-kafka-0-10_2.12-3.2.0.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-tags_2.12-3.2.0.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//spark-token-provider-kafka-0-10_2.12-3.2.0.jar," \
               "file:///C://spark-3.1.2-bin-hadoop3.2//jar_files//unused-1.0.0.jar," \
               "file:///C://spark-3.2.0-bin-hadoop3.2//jars//spark-sql_2.12-3.2.0.jar," \
               "file:///C://spark-3.2.0-bin-hadoop3.2//jars//spark-streaming_2.12-3.2.0.jar"
    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                                       "org.apache.kafka:kafka-clients:2.8.0") \
        .getOrCreate()
        # .config("spark.jars",SparkJar) \
        # .config("spark.executor.extraClassPath", SparkJar) \
        # .config("spark.executor.extraLibrary", SparkJar) \
        # .config("spark.driver.extraClassPath", SparkJar) \
        # .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Printing Schema of trade_detail_df: ")
    transaction_detail_df.printSchema()

    transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    transaction_detail_schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("transaction_card_type", StringType()) \
        .add("transaction_amount", StringType()) \
        .add("transaction_datetime", StringType())

    transaction_detail_df2 = transaction_detail_df1\
        .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")

    transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    trans_detail_write_stream3 = transaction_detail_df3 \
        .writeStream \
        .trigger(processingTime='2 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    # trans_detail_write_stream3.awaitTermination()

    # print(trade_detail.collect())

    # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")\
        .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
        col("sum(transaction_amount)").alias("total_transaction_amount"))

    print("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))\
                                                    .withColumn("value", concat(lit("{'transaction_card_type': '"), \
                                                    col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
                                                    col("total_transaction_amount").cast("string"), lit("'}")))

    print("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()

    # # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df5 \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
    trans_detail_write_stream.awaitTermination()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trans_detail_write_stream_1 = transaction_detail_df5 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint") \
        .start()
        #  .option("path","") \

    trans_detail_write_stream_1.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")