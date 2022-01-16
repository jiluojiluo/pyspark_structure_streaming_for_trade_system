# -*- coding:utf-8 -*-
"""
作者：luoji
日期：2022年01月17日
"""

import pymysql

#打开数据库连接
db = pymysql.connect(host="localhost",user="root",password="root",database="sparkStreaming",port=3306)
#获取游标
cursor=db.cursor()
print(cursor)

# #创建pythonBD数据库
# cursor.execute('CREATE DATABASE IF NOT EXISTS sparkStreaming DEFAULT CHARSET utf8 COLLATE utf8_general_ci;')
# cursor.close()#先关闭游标
# conn.close()#再关闭数据库连接
# print('创建pythonBD数据库成功')

# #创建user表
# cursor.execute('drop table if exists user')
# sql="""CREATE TABLE IF NOT EXISTS `user` (
# 	  `id` int(11) NOT NULL AUTO_INCREMENT,
# 	  `name` varchar(255) NOT NULL,
# 	  `age` int(11) NOT NULL,
# 	  PRIMARY KEY (`id`)
# 	) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=0"""
#
# cursor.execute(sql)
# cursor.close()#先关闭游标
# conn.close()#再关闭数据库连接
# print('创建数据表成功')


cursor.execute("SELECT * FROM sparkstreaming.stockbook;")
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
sql = "INSERT INTO sparkstreaming.stockBook(securityId, investorId, stockBookUnit,remainingBalance," \
      "remainingBalanceBefore,transactionFlag,bookDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
# 一个tuple或者list
data = [
    ('000001', '1000000001', '000001',1000.0,8000.0,'S','2022-01-16'),
    ('000002', '1000000001', '000001',2000.0,3000.0,'S','2022-01-16'),
    ('000003', '1000000001', '000001',2100.0,3100.0,'S','2022-01-16'),
    ('000004', '1000000001', '000001',12000.0,2100.0,'B','2022-01-16'),
    ('000001', '0000000002', '000001',1000.0,8000.0,'S','2022-01-16'),
    ('000002', '0000000002', '000001',2000.0,3000.0,'S','2022-01-16'),
    ('000003', '0000000002', '000001',2100.0,3100.0,'S','2022-01-16'),
    ('000004', '0000000002', '000001',12000.0,2100.0,'B','2022-01-16'),
    ('000001', '1000000005', '000002',1000.0,8000.0,'S','2022-01-16'),
    ('000002', '0000000005', '000002',2000.0,3000.0,'S','2022-01-16'),
    ('000003', '0000000005', '000002',2100.0,3100.0,'S','2022-01-16'),
    ('000004', '0000000005', '000002',12000.0,2100.0,'B','2022-01-16'),
    ('000001', '0000000002', '000003',1000.0,8000.0,'S','2022-01-16'),
    ('000002', '0000000002', '000003',2000.0,3000.0,'S','2022-01-16'),
    ('000003', '0000000002', '000003',2100.0,3100.0,'S','2022-01-16'),
    ('000004', '0000000002', '000003',12000.0,2100.0,'B','2022-01-16'),
     ]

# data = [('000001', '0000000005', '000001',1000.0,8000.0,'S','2022-01-16')]
try:
    # 执行sql语句
    cursor.executemany(sql,data)
    # 提交到数据库执行
    db.commit()
except Exception as r:
    print("chu xian yi chang")
    print('未知错误 %s' % (r))
    db.commit()
cursor.close()
db.close()
print('mysql done')