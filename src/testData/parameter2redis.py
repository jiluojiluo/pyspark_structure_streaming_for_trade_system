# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
function: 把交易单元和记账单元的一一对应关系，保存到redis，供sparkStreaming调用。
"""

import redis

parameterRedis = redis.StrictRedis(host='localhost', port=6379, db=0)
preString = 'tradeUnit:'

for i in range(10000):
    bookUnit = '600000'
    tradeUnit = '000000'
    stri = str(i)
    lenStr = len(stri)
    bookUnit = bookUnit[0:(6-lenStr):1] + stri
    print('bookUnit:',bookUnit)
    tradeUnit = tradeUnit[0:(6 - lenStr):1] + stri
    print('tradeUnit:', tradeUnit)
    redisKey = preString + tradeUnit
    redisValue = bookUnit
    parameterRedis.set(redisKey, redisValue)
