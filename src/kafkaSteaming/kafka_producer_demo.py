# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "testtopic"
# KAFKA_INPUT_TOPIC_NAME_CONS = "testtopic2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    # message = {}
    # message["name"] = 'peipei'
    # message["age"] = '32'
    # message["sex"] = 'male'
    # print(message)
    # kafka_producer_obj.send(KAFKA_INPUT_TOPIC_NAME_CONS, message)
    # time.sleep(1)

    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    message = None
    for i in range(500):
        i = i + 1
        message = {}
        print("Sending message to Kafka topic: " + str(i))
        event_datetime = datetime.now()

        message["transaction_id"] = str(i)
        message["transaction_card_type"] = random.choice(transaction_card_type_list)
        message["transaction_amount"] = round(random.uniform(5.5,555.5), 2)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        print("Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(3)