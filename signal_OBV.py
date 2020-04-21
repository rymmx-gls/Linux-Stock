# coding:utf8

from lib.obv import obv
import pandas as pd
from settings import engine_ts
ts_code = "002480.SZ"
sql = "select trade_date,close,vol from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)
df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
df = df[::-1]
df["OBV"] = obv(close= df.close, volume=df.vol, offset=None)
print df
import matplotlib.pyplot as plt
df.plot(x='trade_date', kind='line', figsize=(20, 8), grid=True)
plt.show()