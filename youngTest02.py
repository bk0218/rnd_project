import sys
import cx_Oracle
import os
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
import pandas as pd
import numpy as np

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

#CPS_SCHOOL_EMPLOY_STD_INFO (교내외 채용정보 신청학생-관리자 등록후 학생이 신청 테이블) 가져오기
CPS_SCHOOL_EMPLOY_STD_INFO = conn.cursor()
CPS_SCHOOL_EMPLOY_STD_INFO.execute('SELECT * FROM CPS_SCHOOL_EMPLOY_STD_INFO')

# 커서 rowfactory로 지정
CPS_SCHOOL_EMPLOY_STD_INFO.rowfactory = makeDictFactory(CPS_SCHOOL_EMPLOY_STD_INFO)

# 테이블에서 데이터 가져오기
results = CPS_SCHOOL_EMPLOY_STD_INFO.fetchall()

while results:
    print("SCE_STD_KEY_ID = %%" * (results[0]))
    results = CPS_SCHOOL_EMPLOY_STD_INFO.fetchall()


CPS_SCHOOL_EMPLOY_STD_INFO.close()

#DB close
conn.close()
