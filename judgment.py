# coding:utf8
import numpy as np
import pandas as pd
from lib.ema import ema
from lib.macd_rymmx import macd


from settings import engine_ts

sql = "select trade_date,close from `daily_002480.SZ` order by trade_date DESC limit 500;"

df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)

df = macd(df)
# 黄金交叉
df["MACD_bug"] = np.where((df['DIF']-df['DEA']>0)&(df['DIF']>0), 1, 0)
df["MACD_sell"] = np.where(df['DIF']-df['DEA']<0, -1, 0)

# df['MACD_judgment']=df.apply(lambda df:1 if df['DIF'] - df['DEA'] > 0 else 0,axis=1)


# def jedg(df):
#     if df['DIF'] - df['DEA']: return 1
#     else: return 0
#
# df['MACD_judgment']=df.apply(jedg,axis=1)

print df.filter(['trade_date','close', 'BAR', 'MACD_bug', 'MACD_sell'])
