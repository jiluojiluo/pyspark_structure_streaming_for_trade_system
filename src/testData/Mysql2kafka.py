import numpy as np

import pymysql
import time
from kafka import KafkaProducer
from datetime import datetime
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "tradingDetail"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                   value_serializer=lambda x: dumps(x).encode('utf-8'))

db = pymysql.connect(host="localhost", user="root", password="root", database="sparkStreaming", port=3306)
# 获取游标
cursor = db.cursor()
cursor.execute("SELECT * FROM sparkstreaming.stockbook;")
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
        message["securityId"] = res[0]
        message["investorId"] = res[1]
        message["stockBookUnit"] = res[2]
        message["remainingBalance"] = float(res[3])
        message["remainingBalanceBefore"] = float(res[4])
        message["transactionFlag"] = res[5]
        message["bookdate"] = str(res[6])

        print("Sending message to Kafka topic,Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        # 控制发送速率
        time.sleep(1)
cursor.close()
db.commit()
db.close()
print('sql执行成功')
