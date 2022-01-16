# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""

import pymysql
import time
from kafka import KafkaProducer
from datetime import datetime
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "tradeDetail"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                   value_serializer=lambda x: dumps(x).encode('utf-8'))

db = pymysql.connect(host="localhost", user="root", password="root", database="sparkStreaming", port=3306)
# 获取游标
cursor = db.cursor()
cursor.execute("SELECT * FROM sparkstreaming.tradeDetail;")
while 1:
    res = cursor.fetchone()
    if res is None:
        # 表示已经取完结果集
        break
    else:
        print(res)
        # 赋值到kafka
        message = {}
        event_datetime = datetime.now()
        message["id"] = res[0]
        message["securityId"] = res[1]
        message["investorId"] = res[2]
        message["tradeUnit"] = res[3]
        message["counterpartyInvestorId"] = res[4]
        message["counterpartyTradeUnit"] = res[5]
        message["tradePrice"] = float(res[6])
        message["tradeVolume"] = float(res[7])
        message["tradeType"] = res[8]
        message["tradeTime"] = str(res[9])
        message["tradedate"] = str(res[10])

        print("Sending message to Kafka topic,Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        # 控制发送速率
        time.sleep(2)
cursor.close()
db.commit()
db.close()
print('sql执行成功')
