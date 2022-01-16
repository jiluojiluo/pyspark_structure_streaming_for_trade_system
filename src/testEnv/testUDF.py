import pandas as pd
import redis
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
"""
作者：luoji
日期：2022年01月17日
"""

def avg_score(score, people):
    try:
        if int(people) != 0:
            return int(score) / int(people)
        else:
            return 0
    except:
        return 0


func = udf(avg_score, DoubleType())
#  此处省略df的获取步骤
# lst=[[1,2,3],
#      [4,5,6],
#      [7,8,9]]
# df=pd.DataFrame(data=lst,columns=["id", "total_score", "total_people"])
# df = df.select("id", "total_score", "total_people")
# df = df.withColumn("avg_score",func(df.total_score, df.total_people))


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno", "Name"]
data = [("1", "tradeUnit:000001"),
        ("2", "tradeUnit:000002"),
        ("3", "tradeUnit:000003")]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)


def convertCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr


# pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
# parameterRedis = redis.Redis(connection_pool=pool)

def parameterRedisGet(tradeUnit):
    try:
        parameterRedis = redis.Redis(host='localhost', port=6379,decode_responses=True)
        bookUnit = parameterRedis.get(tradeUnit)
        print(bookUnit)
        return bookUnit

    except:
        print("can't get bookUnit")
        return tradeUnit

from pyspark.sql.functions import udf

# udf1 = udf(convertCase,StringType())
udf1 = udf(parameterRedisGet, StringType())

# df.select(col("Seqno"), \
#     udf1(col("Name")).alias("Name") ) \
#    .show(truncate=False)
#
# df.select(col("Seqno"), \
#     udf1(col("Name")).alias("Name") ) \
#    .show(truncate=False)

df.withColumn("bookUnit", udf1(df.Name)).show()
