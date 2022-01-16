# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""
import time
import datetime
import pymysql


#打开数据库连接
db = pymysql.connect(host="localhost",user="root",password="root",database="sparkStreaming",port=3306)
#获取游标
cursor=db.cursor()
print(cursor)

cursor.execute("SELECT * FROM sparkstreaming.tradeDetail;")
while 1:
    res=cursor.fetchone()
    if res is None:
        #表示已经取完结果集
        break
    print (res)
# cursor.close()
# db.commit()
# db.close()
print('sql执行成功')



# SQL 插入语句
sql = "INSERT INTO sparkstreaming.tradeDetail(securityId, investorId, tradeUnit,counterpartyInvestorId," \
      "counterpartyTradeUnit,tradePrice,tradeVolume,tradeType,tradeTime,tradedate) " \
      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

datetime = datetime.datetime.now()
date = datetime.now().date()
# 一个tuple或者list
data = [
    ('000001', '1000000001', '000001','2000000001','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000002', '1000000001', '000001','2000000002','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000003', '1000000001', '000001','2000000003','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000004', '1000000001', '000001','2000000004','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000001', '0000000002', '000001','2000000005','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000002', '0000000002', '000001','2000000006','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000003', '0000000002', '000001','2000000007','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000004', '0000000002', '000001','2000000008','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000001', '1000000005', '000002','2000000009','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000002', '0000000005', '000002','2000000011','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000003', '0000000005', '000002','2000000012','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000004', '0000000005', '000002','2000000013','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000001', '0000000002', '000003','2000000014','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000002', '0000000002', '000003','2000000015','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000003', '0000000002', '000003','2000000016','000002',100.5,8000.0,'matchmaking',datetime,date),
    ('000004', '0000000002', '000003','2000000017','000002',100.5,8000.0,'matchmaking',datetime,date),
     ]

try:
    # 执行sql语句
    cursor.executemany(sql,data)
    # 提交到数据库执行
    db.commit()
except Exception as r:
    print("error !!!")
    print('未知错误 %s' % (r))
    db.commit()
cursor.close()
db.close()
print('mysql done')