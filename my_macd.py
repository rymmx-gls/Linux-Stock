# coding:utf8

# L12 = 2.0 / (12.0 + 1.0)
# print L12
L12 = 0.1538
# L26 = 2.0 / (26.0 + 1.0)
# print L26
L26 = 0.0741

from settings import session, engine_ts
import pandas as pd

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',100)


sql = "select trade_date,close from `daily_002480.SZ` order by trade_date DESC limit 50;"
df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None,
                       parse_dates=None, chunksize=None)
# print df

from lib.ema import ema

ema12 = ema(close=df['close'][::-1], length=12, offset=None)
ema26 = ema(close=df['close'][::-1], length=26, offset=None)

df['ema12'] = ema12
df['ema26'] = ema26

# MACD
df['DIF'] = df['ema12'] - df['ema26']
df['DEA'] = df['DIF'][::-1].rolling(2).mean()
df['BAR'] = (df['DIF'] - df['DEA'])*2

# MA5, MA10, MA20
df['MA5'] = df['close'][::-1].rolling(5).mean()
df['MA10'] = df['close'][::-1].rolling(10).mean()
df['MA20'] = df['close'][::-1].rolling(20).mean()

print df.filter(['ema12','ema26','trade_date', 'close', 'DIF', 'DEA', 'BAR'])

close = 12.68
EMA12_yesterday = 1.0
EMA26_yesterday = 0.6
EMA12 = L12 * close + (1 - L12) * EMA12_yesterday
print EMA12
EMA26 = L26 * close + (1 - L26) * EMA26_yesterday
print EMA26

DIF = EMA12 - EMA26
DIF_1 = 1
DIF_2 = 1
DIF_3 = 1
DIF_4 = 1
DIF_5 = 1
DIF_6 = 1
DIF_7 = 1
DIF_8 = 1

DEA = (DIF + DIF_1 + DIF_2 + DIF_3 + DIF_4 + +DIF_5 + DIF_6 + DIF_7 + DIF_8) / 9.0
print "DEA", DEA

BAR = (DIF - DEA) * 2
print 'BAR', BAR

judgment = DIF - DEA

if judgment > 0:
    print '--->买入: judgment(%s)=DIF(%s)-DEA(%s)>0' % (judgment, DIF, DEA)

if judgment < 0:
    print 'DIF:', DIF
    print 'DEA:', DEA
    print u'DIF > DEA, 买入'
