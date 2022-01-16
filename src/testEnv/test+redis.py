# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""
from pyspark.sql import SparkSession
from pyspark import SparkContext
import redis

sc1 = SparkSession.builder.appName("test1").getOrCreate()
sc = SparkContext.getOrCreate()
rdd = sc.parallelize(["b", "a", "c", "a"])
print(sorted(rdd.map(lambda x: (x, 1)).collect()))

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
parameterRedis = redis.Redis(connection_pool=pool)
preString = 'tradeUnit:'
tradeUnit = '000001'
redisKey = preString + tradeUnit
bookUnit = parameterRedis.get(redisKey)
print('bookUnit: ',bookUnit)
