# coding:utf8
from sqlalchemy import engine, types
from sqlalchemy.orm import sessionmaker
import tushare as ts

import pymysql
conn = pymysql.connect(host='localhost',port=6033, user='root',password='1257',database='stock',charset='utf8')
cursor = conn.cursor()
# sql='''
# INSERT INTO `trade_cal` ( `exchange`, `cal_date`, `is_open`,`pretrade_date`)
#     SELECT  '000','20200420',1,'20200417'
#     WHERE not exists (select `cal_date` from `trade_cal` where `cal_date` = '20200426')
# '''
# cursor.execute(sql)
# # 关闭光标对象
# cursor.close()
# # 关闭数据库连接
# conn.close()


# import pymysql; pymysql.install_as_MySQLdb()
# db_connect_string="mysql://root:1257@localhost:6033/stock?charset=utf8"
db_connect='mysql+pymysql://root:1257@localhost:6033/stock?charset=utf8'
# engine_ts = engine.create_engine(db_connect, encoding='utf8', echo=True)
engine_ts = engine.create_engine(db_connect, encoding='utf8', echo=False)
Sesssion=sessionmaker(bind=engine_ts)
session=Sesssion()

ts.set_token('b77e3bd1d70fc17bdcaf02e0f13ec8266ec0f15eb312892f32bf69dd')
pro = ts.pro_api()



# ------ from settings import ... ------ #
"""
from settings import pro, engine_ts, session
"""
# ------ from settings import ... ------ #