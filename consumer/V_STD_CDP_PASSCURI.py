#!/usr/bin/python3.6

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from json import dumps

consumer = KafkaConsumer(
     'V_STD_CDP_PASSCURI',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )


client = MongoClient('localhost:27017')
db = client.cpmongo

V_STD_CDP_PASSCURI = db.V_STD_CDP_PASSCURI

for message in consumer:
    V_STD_CDP_PASSCURI.insert_one(message.value)
    print(f"value={message.value}")

    


