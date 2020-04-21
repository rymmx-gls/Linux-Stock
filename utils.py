# coding:utf8
from models import StockBasicModel
from settings import session, engine_ts
import pandas as pd
# objs = session.query(StockBasicModel).all()
# for obj in objs:
#     print obj.ts_code

df = pd.read_sql_table(table_name="daily_000001.SZ", con=engine_ts, schema=None, index_col=None,
                   coerce_float=True, parse_dates=None, columns=None,
                   chunksize=None)
# print df

print df.describe()
print df.ewm
import pandas_ta
#
#
# pandas_ta.
