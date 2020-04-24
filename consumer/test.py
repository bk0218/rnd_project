#!/usr/bin/python3.6

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from json import dumps
import datetime

consumer = KafkaConsumer(
     'CPS_BOARD_REPLY',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )

def getOrigin(result):
    _list = list(result.keys())
    for item in _list:
        if "DATE" in item:
           result[item]=datetime.datetime.strptime(result[item], "%d-%b-%Y (%H:%M:%S.%f)")

    return result

client = MongoClient('localhost:27017')
db = client.cpmongo

CPS_BOARD_REPLY = db.CPS_BOARD_REPLY

for message in consumer:
    print("HELLO")
    print(message.value)
    print(type(message.value))
    data = getOrigin(message.value)
    print(type(data))
    print(data)
    #CPS_BOARD_REPLY.insert_one(data)
    print(f"value={data}")

    


