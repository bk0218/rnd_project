#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-

import cx_Oracle
import os
from json import dumps
from kafka import KafkaProducer
import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'), acks=1)

# JAVA_HOME이 설정되어 있지 않을 경우
if ("JAVA_HOME" not in os.environ):
    os.environ["JAVA_HOME"] = "[C:\Program Files\Java\jdk1.8.0_231]"

# dictionary형태로 return

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))

    return createRow

def getTime(result):
    _list = list(result.keys())
    for item in _list:
        if isinstance(item, datetime.datetime):
           result[item] = result.pop(item).strftime("%d-%b-%Y (%H:%M:%S.%f)")
        if isinstance(result[item], cx_Oracle.LOB):
           result[item] = str(result.pop(item))
    return result

#DB접속하기
conn = cx_Oracle.connect('cpsrndver/cpsrndver123@222.122.47.39:1521/orcl', encoding='euc-kr')
"""
#V_STD_CDP_SREG(학생정보 테이블) 가져오기
V_STD_CDP_SREG = conn.cursor()
V_STD_CDP_SREG.execute('SELECT * FROM V_STD_CDP_SREG')

# 커서 rowfactory로 지정
V_STD_CDP_SREG.rowfactory = makeDictFactory(V_STD_CDP_SREG)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_SREG.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_SREG', value=data)

print('V_STD_CDP_SREG 테이블 전송완료')

V_STD_CDP_SREG.close()

#V_STD_CDP_DEPT(부서정보 테이블) 가져오기
V_STD_CDP_DEPT = conn.cursor()
V_STD_CDP_DEPT.execute('SELECT * FROM V_STD_CDP_DEPT')

# 커서 rowfactory로 지정
V_STD_CDP_DEPT.rowfactory = makeDictFactory(V_STD_CDP_DEPT)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_DEPT.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_DEPT', value=data)

print('V_STD_CDP_DEPT 테이블 전송완료')

V_STD_CDP_DEPT.close()

#V_STD_CDP_STAF(교직원 정보 테이블) 가져오기
V_STD_CDP_STAF = conn.cursor()
V_STD_CDP_STAF.execute('SELECT * FROM V_STD_CDP_STAF')

# 커서 rowfactory로 지정
V_STD_CDP_STAF.rowfactory = makeDictFactory(V_STD_CDP_STAF)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_STAF.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_STAF', value=data)

print('V_STD_CDP_STAF 테이블 전송완료')

V_STD_CDP_STAF.close()

#V_STD_CDP_DEPT(부서정보 테이블) 가져오기
V_STD_CDP_DEPT = conn.cursor()
V_STD_CDP_DEPT.execute('SELECT * FROM V_STD_CDP_DEPT')

# 커서 rowfactory로 지정
V_STD_CDP_DEPT.rowfactory = makeDictFactory(V_STD_CDP_DEPT)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_DEPT.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_DEPT', value=data)

print('V_STD_CDP_DEPT 테이블 전송완료')

V_STD_CDP_DEPT.close()

#V_STD_CDP_STAF(교직원 정보 테이블) 가져오기
V_STD_CDP_STAF = conn.cursor()
V_STD_CDP_STAF.execute('SELECT * FROM V_STD_CDP_STAF')

# 커서 rowfactory로 지정
V_STD_CDP_STAF.rowfactory = makeDictFactory(V_STD_CDP_STAF)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_STAF.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_STAF', value=data)
    
print('V_STD_CDP_STAF 테이블 전송완료')

V_STD_CDP_STAF.close()

#CPS_CODE_MNG (통합 코드관리 테이블) 가져오기
CPS_CODE_MNG = conn.cursor()
CPS_CODE_MNG.execute('SELECT * FROM CPS_CODE_MNG')

# 커서 rowfactory로 지정
CPS_CODE_MNG.rowfactory = makeDictFactory(CPS_CODE_MNG)

# 테이블에서 데이터 가져오기
results = CPS_CODE_MNG.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_CODE_MNG', value=data)


print('CPS_CODE_MNG 테이블 전송완료')

CPS_CODE_MNG.close()

#CPS_GRADUATE_CORP_INFO (졸업_기업_테이블) 가져오기
CPS_GRADUATE_CORP_INFO = conn.cursor()
CPS_GRADUATE_CORP_INFO.execute('SELECT * FROM CPS_GRADUATE_CORP_INFO')

# 커서 rowfactory로 지정
CPS_GRADUATE_CORP_INFO.rowfactory = makeDictFactory(CPS_GRADUATE_CORP_INFO)

# 테이블에서 데이터 가져오기
results = CPS_GRADUATE_CORP_INFO.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_GRADUATE_CORP_INFO', value=data)

print('CPS_GRADUATE_CORP_INFO 테이블 전송완료')

CPS_GRADUATE_CORP_INFO.close()

#CPS_NCR_PROGRAM_INFO (비교과_정보_테이블) 가져오기
CPS_NCR_PROGRAM_INFO = conn.cursor()
CPS_NCR_PROGRAM_INFO.execute('SELECT * FROM CPS_NCR_PROGRAM_INFO')

# 커서 rowfactory로 지정
CPS_NCR_PROGRAM_INFO.rowfactory = makeDictFactory(CPS_NCR_PROGRAM_INFO)

# 테이블에서 데이터 가져오기
results = CPS_NCR_PROGRAM_INFO.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_NCR_PROGRAM_INFO', value=data)

print('CPS_NCR_PROGRAM_INFO 테이블 전송완료')

CPS_NCR_PROGRAM_INFO.close()

#CPS_NCR_PROGRAM_STD (비교과_신청학생_테이블) 가져오기
CPS_NCR_PROGRAM_STD = conn.cursor()
CPS_NCR_PROGRAM_STD.execute('SELECT * FROM CPS_NCR_PROGRAM_STD')

# 커서 rowfactory로 지정
CPS_NCR_PROGRAM_STD.rowfactory = makeDictFactory(CPS_NCR_PROGRAM_STD)

# 테이블에서 데이터 가져오기
results = CPS_NCR_PROGRAM_STD.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_NCR_PROGRAM_STD', value=data)

print('CPS_NCR_PROGRAM_STD 테이블 전송완료')

CPS_NCR_PROGRAM_STD.close()

#V_STD_CDP_SUBJECT(교과 정보) 가져오기
V_STD_CDP_SUBJECT = conn.cursor()
V_STD_CDP_SUBJECT.execute('SELECT * FROM V_STD_CDP_SUBJECT')

# 커서 rowfactory로 지정
V_STD_CDP_SUBJECT.rowfactory = makeDictFactory(V_STD_CDP_SUBJECT)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_SUBJECT.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_SUBJECT', value=data)

print('V_STD_CDP_SUBJECT 테이블 전송완료')

V_STD_CDP_SUBJECT.close()

#V_STD_CDP_PASSCURI (교과목 수료) 가져오기
V_STD_CDP_PASSCURI = conn.cursor()
V_STD_CDP_PASSCURI.execute('SELECT * FROM V_STD_CDP_PASSCURI')

# 커서 rowfactory로 지정
V_STD_CDP_PASSCURI.rowfactory = makeDictFactory(V_STD_CDP_PASSCURI)

# 테이블에서 데이터 가져오기
results = V_STD_CDP_PASSCURI.fetchall()

for result in results:
    data = result
    print(result)
    producer.send('V_STD_CDP_PASSCURI', value=data)

print('V_STD_CDP_PASSCURI 테이블 전송완료')

V_STD_CDP_PASSCURI.close()

#CPS_BOARD_REPLY (비교과/교과 덧글 테이블) 가져오기
CPS_BOARD_REPLY = conn.cursor()
CPS_BOARD_REPLY.execute('SELECT * FROM CPS_BOARD_REPLY')

# 커서 rowfactory로 지정
CPS_BOARD_REPLY.rowfactory = makeDictFactory(CPS_BOARD_REPLY)

# 테이블에서 데이터 가져오기
results = CPS_BOARD_REPLY.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_BOARD_REPLY', value=data)

print('CPS_BOARD_REPLY 테이블 전송완료')

CPS_BOARD_REPLY.close()
"""
#CPS_STAR_POINT (교과/비교과용 별점 테이블) 가져오기
CPS_STAR_POINT = conn.cursor()
CPS_STAR_POINT.execute('SELECT * FROM CPS_STAR_POINT')

# 커서 rowfactory로 지정
CPS_STAR_POINT.rowfactory = makeDictFactory(CPS_STAR_POINT)

# 테이블에서 데이터 가져오기
results = CPS_STAR_POINT.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_STAR_POINT', value=data)

print('CPS_STAR_POINT 테이블 전송완료')

CPS_STAR_POINT.close()
"""
#CPS_OUT_ACTIVITY_MNG (교외 활동 테이블) 가져오기
CPS_OUT_ACTIVITY_MNG = conn.cursor()
CPS_OUT_ACTIVITY_MNG.execute('SELECT * FROM CPS_OUT_ACTIVITY_MNG')

# 커서 rowfactory로 지정
CPS_OUT_ACTIVITY_MNG.rowfactory = makeDictFactory(CPS_OUT_ACTIVITY_MNG)

# 테이블에서 데이터 가져오기
results = CPS_OUT_ACTIVITY_MNG.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_OUT_ACTIVITY_MNG', value=data)

print('CPS_OUT_ACTIVITY_MNG 테이블 전송완료')

CPS_OUT_ACTIVITY_MNG.close()

#CPS_SCHOOL_EMPLOY_INFO (교내외 채용정보-관리자 등록 테이블) 가져오기
CPS_SCHOOL_EMPLOY_INFO = conn.cursor()
CPS_SCHOOL_EMPLOY_INFO.execute('SELECT * FROM CPS_SCHOOL_EMPLOY_INFO')

# 커서 rowfactory로 지정
CPS_SCHOOL_EMPLOY_INFO.rowfactory = makeDictFactory(CPS_SCHOOL_EMPLOY_INFO)

# 테이블에서 데이터 가져오기
results = CPS_SCHOOL_EMPLOY_INFO.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_SCHOOL_EMPLOY_INFO', value=data)

print('CPS_SCHOOL_EMPLOY_INFO 테이블 전송완료')

CPS_SCHOOL_EMPLOY_INFO.close()

#CPS_SCHOOL_EMPLOY_STD_INFO (교내외 채용정보 신청학생-관리자 등록후 학생이 신청 테이블) 가져오기
CPS_SCHOOL_EMPLOY_STD_INFO = conn.cursor()
CPS_SCHOOL_EMPLOY_STD_INFO.execute('SELECT * FROM CPS_SCHOOL_EMPLOY_STD_INFO')

# 커서 rowfactory로 지정
CPS_SCHOOL_EMPLOY_STD_INFO.rowfactory = makeDictFactory(CPS_SCHOOL_EMPLOY_STD_INFO)

# 테이블에서 데이터 가져오기
results = CPS_SCHOOL_EMPLOY_STD_INFO.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_SCHOOL_EMPLOY_STD_INFO', value=data)

print('CPS_SCHOOL_EMPLOY_STD_INFO 테이블 전송완료')

CPS_SCHOOL_EMPLOY_STD_INFO.close()

#CPS_EMPLOY_SEARCH_HIS (채용정보 조회 이력 테이블) 가져오기
CPS_EMPLOY_SEARCH_HIS = conn.cursor()
CPS_EMPLOY_SEARCH_HIS.execute('SELECT * FROM CPS_EMPLOY_SEARCH_HIS')

# 커서 rowfactory로 지정
CPS_EMPLOY_SEARCH_HIS.rowfactory = makeDictFactory(CPS_EMPLOY_SEARCH_HIS)

# 테이블에서 데이터 가져오기
results = CPS_EMPLOY_SEARCH_HIS.fetchall()

for result in results:
    result = getTime(result)
    data = result
    print(result)
    producer.send('CPS_EMPLOY_SEARCH_HIS', value=data)

print('CPS_EMPLOY_SEARCH_HIS 테이블 전송완료')

CPS_EMPLOY_SEARCH_HIS.close()
"""
#DB close
conn.close()

