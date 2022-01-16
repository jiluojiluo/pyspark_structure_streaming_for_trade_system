# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
import redis
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType, StructField, StructType, StringType, IntegerType


KAFKA_INPUT_TOPIC_NAME_CONS = "tradeDetail"
KAFKA_OUTPUT_TOPIC_NAME_CONS_CLEAR = "clearDetail"
KAFKA_OUTPUT_TOPIC_NAME_CONS_BOOK = "bookDetail"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
KAFKA_OUTPUT_TOPIC_NAME_CONS_BOOK_SUM = "bookSum"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Clear Application Started ...")

    # pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    # parameterRedis = redis.Redis(connection_pool=pool)
    preString = 'tradeUnit:'

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka ") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                                       "org.apache.kafka:kafka-clients:2.8.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic: tradeDetail
    trade_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Printing Schema of trade_detail_df: ")
    trade_detail_df.printSchema()

    trade_detail_df_with_timestamp = trade_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the transaction_detail data
    trade_detail_schema = StructType() \
        .add("id", StringType()) \
        .add("securityId", StringType()) \
        .add("investorId", StringType()) \
        .add("tradeUnit", StringType()) \
        .add("counterpartyInvestorId", StringType()) \
        .add("counterpartyTradeUnit", StringType()) \
        .add("tradePrice", StringType()) \
        .add("tradeVolume", StringType()) \
        .add("tradeType", StringType()) \
        .add("tradeTime", StringType()) \
        .add("tradedate", StringType())

    # 从kafka中读取的数据，放在value中，用json反序列化出来
    transaction_detail_df2 = trade_detail_df_with_timestamp \
        .select(from_json(col("value"), trade_detail_schema).alias("trade_detail"), "timestamp")

    trade_detail = transaction_detail_df2.select("trade_detail.*", "timestamp")
    # | -- id: string(nullable=true)
    # | -- securityId: string(nullable=true)
    # | -- investorId: string(nullable=true)
    # | -- tradeUnit: string(nullable=true)
    # | -- counterpartyTradeUnit: string(nullable=true)
    # | -- tradePrice: string(nullable=true)
    # | -- tradeVolume: string(nullable=true)
    # | -- tradeType: string(nullable=true)
    # | -- tradeTime: string(nullable=true)
    # | -- tradedate: string(nullable=true)
    # | -- timestamp: timestamp(nullable=true)
    trade_detail = trade_detail.withColumn("preString", f.lit(preString))
    trade_detail = trade_detail.withColumn("redisKey", f.concat("preString","tradeUnit"))
    trade_detail = trade_detail.withColumn("redisKeyCounterparty", f.concat("preString", "counterpartyTradeUnit"))


    @udf(returnType=StringType())
    def parameterRedisGet(tradeUnit):
        try:
            parameterRedis = redis.Redis(host='localhost', port=6379, decode_responses=True)
            bookUnit = parameterRedis.get(tradeUnit)
            # print(bookUnit)
            return bookUnit

        except:
            print("can't get bookUnit")
            return tradeUnit
    # func = udf(parameterRedisGet,StringType())

    trade_detail = trade_detail.withColumn("bookUnit",parameterRedisGet(trade_detail.redisKey))
    trade_detail = trade_detail.withColumn("counterpartyBookUnit", parameterRedisGet(trade_detail.redisKeyCounterparty))
    trade_detail.printSchema()

    trade_detail = trade_detail.drop(trade_detail.preString)
    trade_detail = trade_detail.drop(trade_detail.redisKey)
    trade_detail = trade_detail.drop(trade_detail.redisKeyCounterparty)
    trade_detail.printSchema()

    trade_detail = trade_detail.withColumn("clearNet",trade_detail.tradePrice*trade_detail.tradeVolume)
    trade_detail.printSchema()

    trans_detail_write_stream3 = trade_detail \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    # trans_detail_write_stream3.awaitTermination(timeout=500)

    # print(trade_detail.collect())
    print("print is over")

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trade_detail = trade_detail.withColumn("key", col("id").cast("string")) \
        .withColumn("value", concat(lit("{'id': '"), col("id"), \
                                    lit("', 'securityId': '"), col("securityId"), \
                                    lit("', 'investorId': '"), col("investorId"), \
                                    lit("', 'tradeUnit': '"), col("tradeUnit"), \
                                    lit("', 'counterpartyInvestorId': '"), col("counterpartyInvestorId"), \
                                    lit("', 'counterpartyTradeUnit': '"), col("counterpartyTradeUnit"), \
                                    lit("', 'tradePrice': '"), col("tradePrice").cast("string"), \
                                    lit("', 'tradeVolume': '"), col("tradeVolume").cast("string"), \
                                    lit("', 'tradeType': '"), col("tradeType"), \
                                    lit("', 'tradeTime': '"), col("tradeTime").cast("string"), \
                                    lit("', 'tradedate': '"), col("tradedate").cast("string"), \
                                    lit("', 'timestamp': '"), col("timestamp").cast("string"), \
                                    lit("', 'bookUnit': '"), col("bookUnit"), \
                                    lit("', 'counterpartyBookUnit': '"), col("counterpartyBookUnit"), \
                                    lit("', 'clearNet: '"), \
                                    col("clearNet").cast("string"), lit("'}")))
    trade_detail.printSchema()

    trade_detail_write_stream_1 = trade_detail \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint2") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS_BOOK) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .start()


    print("send to kafka is over")

    trade_detail_write_stream_1.awaitTermination(timeout=1000)
    trans_detail_write_stream3.awaitTermination(timeout=500)

    clear_detail = trade_detail.select(trade_detail.id,trade_detail.securityId,trade_detail.investorId,
                                       trade_detail.tradeUnit,trade_detail.tradePrice,trade_detail.tradeVolume,
                                       trade_detail.tradeType,trade_detail.tradeTime,trade_detail.tradedate,
                                       trade_detail.timestamp,trade_detail.bookUnit,trade_detail.clearNet)

    clear_detail_counterparty = trade_detail.select(trade_detail.id,trade_detail.securityId,
                                                    trade_detail.counterpartyInvestorId,
                                       trade_detail.counterpartyTradeUnit,trade_detail.tradePrice,-trade_detail.tradeVolume,
                                       trade_detail.tradeType,trade_detail.tradeTime,trade_detail.tradedate,
                                       trade_detail.timestamp,trade_detail.counterpartyBookUnit,-trade_detail.clearNet)
    # tradeUnits = trade_detail.select('tradeUnit').alias("tradeUnit")
    # redisKey = preString + tradeUnits
    # bookUnit = parameterRedis.get(redisKey)
    # print('bookUnit: ', bookUnit)
    # clear_detail = trade_detail.select(bookUnit,'bookUnit')

    # # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    # clear_detail_write_stream = clear_detail \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint") \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS_CLEAR) \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .start()
    #
    # clear_detail_write_stream.awaitTermination()
    #
    # clear_detail_write_stream_counterparty = clear_detail_counterparty \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint") \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS_CLEAR) \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .start()
    #
    # clear_detail_write_stream_counterparty.awaitTermination()





    # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    transaction_detail_df4 = trade_detail.groupBy("securityId") \
        .agg({'tradeVolume': 'sum'}).select("securityId", \
                                            col("sum(tradeVolume)").alias("total_transaction_amount"))

    print("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100)) \
        .withColumn("value", concat(lit("{'securityId': '"), \
                                    col("securityId"), lit("', 'total_transaction_amount: '"), \
                                    col("total_transaction_amount").cast("string"), lit("'}")))

    print("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()

    # # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df5 \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    trans_detail_write_stream.awaitTermination(timeout=60)

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trans_detail_write_stream_1 = transaction_detail_df5 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .option("checkpointLocation", "file:///C://LuojiPythonProject//SparkForClearSystem//py_checkpoint") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS_BOOK_SUM) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .start()
    #  .option("path","") \

    trans_detail_write_stream_1.awaitTermination(timeout=60)

    print("PySpark Structured Streaming with Kafka Application Completed.")
