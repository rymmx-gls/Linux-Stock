# -*- coding: utf-8 -*-
import time

import numpy as np
import pandas as pd
from lib.ema import ema
from lib.macd_rymmx import macd


from settings import engine_ts
from test_email import email_to_me

ts_code = "000001.SZ"


sql = "select trade_date,close from `daily_{}` order by trade_date DESC limit 200;".format(ts_code)

df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
df = df[::-1]
# print df
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)

df = macd(df)
# 黄金交叉
df["MACD_buy"] = np.where((df['DIF']-df['DEA']>0)&(df['DIF']>0), 1, 0)
df["MACD_sell"] = np.where(df['DIF']-df['DEA']<0, -1, 0)
del df['ema12']
del df['ema26']
del df['DIF']
del df['DEA']
del df['BAR']

# 盈利情况
df['BUY_signal'] = np.where((df['MACD_buy'].rolling(2, min_periods=2).sum()==1)&(df['MACD_buy']==1),-df.close,0)
df['SELL_signal'] = np.where((df['MACD_sell'].rolling(2, min_periods=2).sum()==-1)&(df['MACD_sell']==-1),df.close,0)
# del df["MACD_buy"]
# del df["MACD_sell"]
# del df["BUY_signal"]
# del df["SELL_signal"]
# df['porfix'] = df['MACD_bug'].rolling(2, min_periods=2).sum()
df['porfit'] = df['BUY_signal'].cumsum()+df['SELL_signal'].cumsum()

for i in df.to_dict('records'):
    if i['BUY_signal'] < 0:
        text = u'%s 买入: %s'%(i['trade_date'],ts_code)
        email_to_me(text)
    if i['SELL_signal'] > 0:
        text = u'%s 卖出: %s'%(i['trade_date'],ts_code)
        email_to_me(text)


# del df['MACD_bug']
# del df['MACD_sell']

print df
#
# import matplotlib.pyplot as plt
#
# df.plot(x='trade_date', kind='line', figsize=(20,8), grid=True)
# plt.show()

# def jedg(df):
#     if df['DIF'] - df['DEA']: return 1
#     else: return 0
#
# df['MACD_judgment']=df.apply(jedg,axis=1)

# print df.filter(['trade_date','close', 'BAR', 'MACD_bug', 'MACD_sell'])

# df.to_sql('judg_%s' % ts_code, engine_ts, index=False, if_exists='replace', chunksize=5000)
# time.sleep(10)

# plt.figure(figsize=(18,8))
# ax1 = plt.subplot(2,1,1)
# ax1.plot(df['close'])
# ax2 = plt.subplot(2,1,2)
# ax2.plot(df['close'])
# plt.show()

# df.plot()
# ax1 = plt.subplot(2,1,1)
# ax1.plot(df['close'])
# import matplotlib.pyplot as plt
# from matplotlib import axis
# # df.plot(x='trade_date', kind='line', figsize=(20,8), grid=True, subplots=True)
# df.plot(x='trade_date', kind='line', figsize=(20,8), grid=True)
# # plt.plot(df)
# # df.plot("MACD_bug", 'MACD_sell', kind='scatter')
# plt.show()


