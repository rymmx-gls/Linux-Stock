# -*- coding: utf-8 -*-
import time

import numpy as np
import pandas as pd
from lib.ema import ema
from lib.macd_rymmx import macd
import matplotlib.pyplot as plt


from settings import engine_ts
from utils_rymmx import email_to_me

ts_code = "000001.SZ"


sql = "select trade_date,close from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)

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

df_base = pd.concat([df['trade_date'], df['close']], axis=1)
df_base.plot(x='trade_date', kind='line', grid=True, figsize=(30,10))

df_macd =  pd.concat([df['trade_date'], df['DIF'], df['DEA'], df['BAR']], axis=1)
df_macd.plot(x='trade_date', kind='line', grid=True, figsize=(30,10))

# plt.figure(figsize=(30, 10), dpi=100)
# df.plot(x='trade_date', kind='line', grid=True, figsize=(30,10))

# plt.xlabel("trade_date")
# plt.ylabel("close")
plt.gcf().autofmt_xdate()
plt.show()
# print df
# 黄金交叉
# df["MACD_buy"] = np.where((df['DIF']-df['DEA']>0)&(df['DIF']>0), 1, 0)
# df["MACD_sell"] = np.where(df['DIF']-df['DEA']<0, -1, 0)
# del df['ema12']
# del df['ema26']
# del df['DIF']
# del df['DEA']
# del df['BAR']

# 盈利情况
# df['BUY_signal'] = np.where((df['MACD_buy'].rolling(2, min_periods=2).sum()==1)&(df['MACD_buy']==1),-df.close,0)
# df['SELL_signal'] = np.where((df['MACD_sell'].rolling(2, min_periods=2).sum()==-1)&(df['MACD_sell']==-1),df.close,0)
# del df["MACD_buy"]
# del df["MACD_sell"]
# del df["BUY_signal"]
# del df["SELL_signal"]
# df['porfix'] = df['MACD_bug'].rolling(2, min_periods=2).sum()
# df['porfit'] = df['BUY_signal'].cumsum()+df['SELL_signal'].cumsum()
# today = time.strftime("%Y%m%d", time.localtime())
# print 'today:%s'%today
# for i in df.to_dict('records'):
#     print 'trade_date:%s'%i['trade_date']
#     if i['trade_date'] == today:
#         if i['BUY_signal'] < 0:
#             text = u'%s 买入: %s'%(i['trade_date'],ts_code)
#             email_to_me(text)
#         if i['SELL_signal'] > 0:
#             text = u'%s 卖出: %s'%(i['trade_date'],ts_code)
#             email_to_me(text)
#     else:
#         print "往期"


# del df['MACD_bug']
# del df['MACD_sell']

# print df
#



# 1）创建画布(容器层),并设置画布属性
# plt.figure(figsize=(30, 10), dpi=100)
# 2）绘制折线图(图像层)



# 增加以下两行代码
# 构造x轴刻度标签
# x_ticks_label = df['trade_date'].to_list()
# # print x_ticks_label
# # 构造y轴刻度
# y_ticks = df['close'].to_list()
# print y_ticks

# plt.plot(x_ticks_label, y_ticks)
# plt.xlabel("trade_date")
# plt.ylabel("close")
# plt.gcf().autofmt_xdate()
#
# plt.plot(x, open_list, label='open', linewidth=1, color='red', marker='o', markerfacecolor='blue', markersize=2)
# plt.plot(x, high_list, label='high', linewidth=1, color='green', marker='o', markerfacecolor='blue', markersize=2)
# plt.plot(x, close_list, label='close', linewidth=1, color='blue', marker='o', markerfacecolor='blue', markersize=2)
# plt.plot(x, low_list, label='low', linewidth=1, color='black', marker='o', markerfacecolor='blue', markersize=2)


# # 修改x,y轴坐标的刻度显示
# plt.xticks(x_ticks_label[::5])
# plt.yticks(y_ticks[::5])
# 3）显示图像
# plt.show()

# 2）保存图片到指定路径
# plt.savefig("test.png")
# xticks=df['trade_date'].to_list()
# # print xticks
# df.plot(x='trade_date', kind='line', grid=True, xticks=xticks)
# plt.show()

# def jedg(df):
#     if df['DIF'] - df['DEA']: return 1
#     else: return 0
#
# df['MACD_judgment']=df.apply(jedg,axis=1)
#
# print df.filter(['trade_date','close', 'BAR', 'MACD_bug', 'MACD_sell'])
#
# df.to_sql('judg_%s' % ts_code, engine_ts, index=False, if_exists='replace', chunksize=5000)
# time.sleep(10)

# plt.figure(figsize=(18,8))
# ax1 = plt.subplot(2,1,1)
# ax1.plot(df['close'])
# ax2 = plt.subplot(2,1,2)
# ax2.plot(df['close'])
# plt.show()
#
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


