# coding:utf8
from sqlalchemy import engine, types
from sqlalchemy.orm import sessionmaker
import tushare as ts


# import pymysql; pymysql.install_as_MySQLdb()
# db_connect_string="mysql://root:1257@localhost:6033/stock?charset=utf8"
db_connect='mysql+pymysql://root:1257@localhost:6033/stock?charset=utf8'
# engine_ts = engine.create_engine(db_connect, encoding='utf8', echo=True)
engine_ts = engine.create_engine(db_connect, encoding='utf8', echo=False)
Sesssion=sessionmaker(bind=engine_ts)
session=Sesssion()

ts.set_token('b77e3bd1d70fc17bdcaf02e0f13ec8266ec0f15eb312892f32bf69dd')
pro = ts.pro_api()





