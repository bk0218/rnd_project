#!/usr/bin/python3.6

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from json import dumps

consumer = KafkaConsumer(
     'V_STD_CDP_SREG',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )


client = MongoClient('localhost:27017')
db = client.cpmongo

#V_STD_CDP_SREG = db.V_STD_CDP_SREG

V_STD_CDP_SREG = db.V_STD_CDP_SREG2
for message in consumer:
    #print(V_STD_CDP_SREG.find_one({})["STD_NO"]) 학번 하나만 계속 조회됨. 매번 새로 신청되는 듯.
    if message.value:
       try:
           V_STD_CDP_SREG.find_one({"STD_NO":message.value["STD_NO"]},{"_id":0, "STD_NO":1})["STD_NO"]
           print("EXISTENT, SKIPPING INSERTION")
  
       except:
           print("NON EXISTENT")
           V_STD_CDP_SREG.insert_one(message.value)
           print("INSERTED")
           continue

    """
    if V_STD_CDP_SREG.find({message.value["STD_NO"]}):
       V_STD_CDP_SREG.insert_one(message.value)
       print(f"value={message.value}")
    """
