# coding:utf8

from lib.stoch import stoch
import pandas as pd
from settings import engine_ts
ts_code = "000001.SZ"
sql = "select trade_date,close,high,low from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)
df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
df = df[::-1]
kdj = stoch(close=df.close, high=df.high, low=df.low, fast_k=None, slow_k=None, slow_d=None, offset=None)
df = pd.concat([df, kdj], axis=1)
# print df
import matplotlib.pyplot as plt
df.plot(x='trade_date', kind='line', figsize=(20, 8), grid=True)
plt.show()



