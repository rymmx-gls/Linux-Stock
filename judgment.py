import pandas as pd
from lib.ema import ema
from lib.macd_rymmx import macd

from settings import engine_ts

sql = "select trade_date,close from `daily_002480.SZ` order by trade_date DESC limit 50;"


df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)

# df.iterrows()


df = macd(df)

print df