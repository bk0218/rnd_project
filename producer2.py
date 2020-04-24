#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-

import cx_Oracle
import os
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# JAVA_HOME이 설정되어 있지 않을 경우
if ("JAVA_HOME" not in os.environ):
    os.environ["JAVA_HOME"] = "[C:\Program Files\Java\jdk1.8.0_231]"

# dictionary형태로 return

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))

    return createRow

#DB접속하기
conn = cx_Oracle.connect('cpsrndver/cpsrndver123@222.122.47.39:1521/orcl', encoding='UTF-8')


#CPS_CODE_MNG (통합 코드관리 테이블) 가져오기
CPS_CODE_MNG = conn.cursor()
CPS_CODE_MNG.execute('SELECT * FROM CPS_CODE_MNG')

# 커서 rowfactory로 지정
CPS_CODE_MNG.rowfactory = makeDictFactory(CPS_CODE_MNG)

# 테이블에서 데이터 가져오기
results = CPS_CODE_MNG.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('CPS_CODE_MNG', value=data)

print('CPS_CODE_MNG 테이블 전송완료')

CPS_CODE_MNG.close()

