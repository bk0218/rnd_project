#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-
import sys
import cx_Oracle
import os
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
import pandas as pd

sys.path.insert(1, '/root/rnd_project/consumer')
from func import getSerial, getOrigin, makeDictFactory

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# JAVA_HOME이 설정되어 있지 않을 경우
if ("JAVA_HOME" not in os.environ):
    os.environ["JAVA_HOME"] = "[C:\Program Files\Java\jdk1.8.0_231]"

# dictionary형태로 return(moved to func.py at consumer file)

#DB접속하기
conn = cx_Oracle.connect('cpsrndver/cpsrndver123@222.122.47.39:1521/orcl', encoding='euc-kr')
conn2 = cx_Oracle.connect('cpsrndver/cpsrndver123@222.122.47.39:1521/orcl', encoding='utf-8')

#CPS_EMPLOY_SEARCH_HIS (채용정보 조회 이력 테이블) 가져오기
CPS_EMPLOY_SEARCH_HIS = conn.cursor()
CPS_EMPLOY_SEARCH_HIS.execute('SELECT * FROM CPS_EMPLOY_SEARCH_HIS')

# 커서 rowfactory로 지정
CPS_EMPLOY_SEARCH_HIS.rowfactory = makeDictFactory(CPS_EMPLOY_SEARCH_HIS)

# 테이블에서 데이터 가져오기
results = CPS_EMPLOY_SEARCH_HIS.fetchall()

for result in results:
    result = getSerial(result)
    for df in result:
        df = pd.DataFrame(columns=[])
        df2 = df.drop_duplicates(' ', keep='last')
        print(df2)
    else:
        data = result
    print(result)
    producer.send('CPS_EMPLOY_SEARCH_HIS', value=data)

print('CPS_EMPLOY_SEARCH_HIS 테이블 전송완료')

CPS_EMPLOY_SEARCH_HIS.close()

#DB close
conn.close()
